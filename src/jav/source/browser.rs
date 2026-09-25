//! Headless-browser cookie minting.
//!
//! Cloudflare's managed challenge runs a JavaScript VM and a pile of browser
//! fingerprint checks, so no HTTP client can answer it. The only reliable way
//! to obtain a `cf_clearance` cookie is to let a real browser load the page,
//! then lift the cookie out of it and hand it to the fast `wreq` client.
//!
//! Only cookie *minting* goes through the browser; page and segment downloads
//! stay on the much faster `wreq` path. Chromium stays alive between mints, but
//! each mint gets an isolated context and a short-lived DevTools connection.
//! Disposing both leaves no challenge pages or transport polling threads idle.
//! Between mints, background services and spare-renderer prewarming are disabled;
//! a one-shot memory-pressure notification asks Chromium to release idle caches.
//!
//! Two details are load-bearing and easy to get wrong:
//!
//! * The browser stays **headless** (the only option inside a container), but
//!   it must *not* send Chrome's default headless agent string — that contains
//!   `HeadlessChrome`, which Cloudflare rejects outright even though the rest
//!   of the fingerprint is genuine. We override it with the same normal Chrome
//!   user agent the HTTP client sends, so the cookie Cloudflare issues is bound
//!   to the string we use later. Challenge completion still depends on the
//!   site, browser version, and network; failures are reported to the caller.
//! * `headless_chrome` is synchronous, so every call runs on a blocking thread
//!   instead of stalling the async runtime.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStderr, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use headless_chrome::browser::transport::{SessionId, Transport};
use headless_chrome::browser::{DEFAULT_ARGS, default_executable};
use headless_chrome::protocol::cdp::{Emulation, Memory, Network, Page, Target};

use crate::jav::source::cf::MintedCookie;

/// Tunables for the minting browser.
#[derive(Debug, Clone)]
pub struct BrowserOptions {
    /// Explicit Chromium/Chrome binary. Empty lets the crate auto-detect one.
    pub executable: PathBuf,
    /// Parent directory for process-owned profiles; cookies use disposable contexts.
    pub user_data_dir: PathBuf,
    /// Page the challenge is solved against.
    pub target_url: String,
    /// How long to wait for the interstitial to clear.
    pub nav_timeout: Duration,
    /// User agent for both the browser and the HTTP client.
    pub user_agent: String,
}

impl BrowserOptions {
    pub fn new(
        user_data_dir: impl Into<PathBuf>,
        target_url: impl Into<String>,
        user_agent: impl Into<String>,
    ) -> Self {
        Self {
            executable: PathBuf::new(),
            user_data_dir: user_data_dir.into(),
            target_url: target_url.into(),
            nav_timeout: Duration::from_secs(150),
            user_agent: user_agent.into(),
        }
    }
}

/// Owns a lazily launched browser and mints cookies with it.
///
/// Exclusivity is already guaranteed by the [`crate::jav::source::cf::CookieStore`] gate, so
/// only one mint can be in flight; the mutex exists so the cached browser can be
/// moved onto the blocking thread that drives it.
pub struct BrowserMinter {
    opts: BrowserOptions,
    slot: Arc<Mutex<Option<Process>>>,
    running: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
}

impl BrowserMinter {
    pub fn new(opts: BrowserOptions) -> Self {
        Self {
            opts,
            slot: Arc::new(Mutex::new(None)),
            running: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A [`crate::jav::source::cf::MintFn`] bound to this minter.
    pub fn mint_fn(self: &Arc<Self>) -> crate::jav::source::cf::MintFn {
        let this = Arc::clone(self);
        Arc::new(move |ua: String| {
            let this = Arc::clone(&this);
            Box::pin(async move { this.mint(ua).await })
        })
    }

    /// True when a browser is currently loaded.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Killing/reaping Chromium and waiting for a mint never block a Tokio worker.
    pub async fn shutdown(&self) {
        self.stopped.store(true, Ordering::Release);
        let slot = Arc::clone(&self.slot);
        let running = Arc::clone(&self.running);
        let _ = tokio::task::spawn_blocking(move || {
            if let Ok(mut browser) = slot.lock() {
                browser.take();
            }
            running.store(false, Ordering::Release);
        })
        .await;
    }

    async fn mint(&self, ua_hint: String) -> Result<MintedCookie, String> {
        let user_agent = if ua_hint.trim().is_empty() {
            self.opts.user_agent.clone()
        } else {
            ua_hint
        };
        let opts = self.opts.clone();
        let slot = Arc::clone(&self.slot);
        let running = Arc::clone(&self.running);
        let stopped = Arc::clone(&self.stopped);
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel_on_drop = CancelMint(Arc::clone(&cancelled));
        tokio::task::spawn_blocking(move || {
            mint_blocking(slot, opts, user_agent, &running, &cancelled, &stopped)
        })
        .await
        .map_err(|e| format!("the browser task failed: {e}"))?
    }
}

/// Dropping the async waiter also stops the blocking poll loop.
struct CancelMint(Arc<AtomicBool>);
impl Drop for CancelMint {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Reuse the process, connecting DevTools only while a mint is active.
fn mint_blocking(
    slot: Arc<Mutex<Option<Process>>>,
    opts: BrowserOptions,
    user_agent: String,
    running: &AtomicBool,
    cancelled: &AtomicBool,
    stopped: &AtomicBool,
) -> Result<MintedCookie, String> {
    let mut guard = slot
        .lock()
        .map_err(|_| "browser slot poisoned".to_string())?;
    if cancelled.load(Ordering::Acquire) || stopped.load(Ordering::Acquire) {
        return Err("cookie mint cancelled".into());
    }
    let connection = guard.as_ref().and_then(|process| match connect(process) {
        Ok(connection) => Some(connection),
        Err(error) => {
            log::warn!("cached Chromium is unavailable; restarting it: {error}");
            None
        }
    });
    let connection = match connection {
        Some(connection) => connection,
        None => {
            guard.take();
            running.store(false, Ordering::Release);
            *guard = Some(launch(&opts, &user_agent)?);
            running.store(true, Ordering::Release);
            connect(guard.as_ref().expect("just launched"))?
        }
    };
    let started = Instant::now();
    let minted = mint_with_browser(&connection.0, &opts, &user_agent, cancelled, stopped)?;
    log::info!(
        "cf_clearance minted in {:.1}s via {}",
        started.elapsed().as_secs_f64(),
        describe(&opts)
    );
    Ok(minted)
}

/// Transport's own Drop does not stop its threads; always shut it down explicitly.
struct MintConnection(Transport);

impl Drop for MintConnection {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

/// Own only the OS process while idle, without a browser event loop or socket.
struct Process {
    child: BrowserChild,
    debug_ws_url: url::Url,
    // Dropped after the child is killed and reaped. Never reuse locks left by
    // an older container, or remove locks belonging to another live process.
    _profile: tempfile::TempDir,
}

struct BrowserChild(Child);

impl Drop for BrowserChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// Chromium writes directly to stderr, outside RUST_LOG. Preserve its diagnostics
// except two known failures of optional Google messaging registration, which this
// cookie-only browser does not use. Keep them available at debug level.
fn forward_browser_stderr(stderr: ChildStderr) {
    let mut reader = BufReader::new(stderr);
    let mut bytes = Vec::new();
    loop {
        bytes.clear();
        match reader.read_until(b'\n', &mut bytes) {
            Ok(0) => break,
            Ok(_) => {
                let line = String::from_utf8_lossy(&bytes);
                let registration_noise = line
                    .contains("google_apis/gcm/engine/registration_request.cc:")
                    && [
                        "Registration response error message: DEPRECATED_ENDPOINT",
                        "Registration response error message: PHONE_REGISTRATION_ERROR",
                    ]
                    .iter()
                    .any(|message| line.trim_end().ends_with(message));
                if registration_noise {
                    log::debug!("{}", line.trim_end());
                } else {
                    let _ = std::io::stderr().lock().write_all(&bytes);
                }
            }
            Err(error) => {
                log::warn!("cannot read Chromium diagnostics: {error}");
                break;
            }
        }
    }
}

fn connect(process: &Process) -> Result<MintConnection, String> {
    Transport::new(
        process.debug_ws_url.clone(),
        Some(process.child.0.id()),
        Duration::from_secs(30),
        None,
    )
    .map(MintConnection)
    .map_err(|e| format!("cannot connect to Chromium: {e}"))
}

fn launch(opts: &BrowserOptions, user_agent: &str) -> Result<Process, String> {
    std::fs::create_dir_all(&opts.user_data_dir).map_err(|e| {
        format!(
            "cannot create browser profile {}: {e}",
            opts.user_data_dir.display()
        )
    })?;

    log::info!("launching the cookie-minting browser ({})", describe(opts));
    let mut args: Vec<std::ffi::OsString> = vec![
        "--disable-gpu".into(),
        "--no-first-run".into(),
        "--no-startup-window".into(),
        "--no-default-browser-check".into(),
        "--disable-background-networking".into(),
        // This process only mints cookies; it needs no background component
        // downloads or domain-reliability uploads between challenges.
        "--disable-component-update".into(),
        "--disable-domain-reliability".into(),
        // Web push is not needed to render a challenge or obtain its cookies.
        "--disable-notifications".into(),
        "--disable-blink-features=AutomationControlled".into(),
        // Crashpad cannot write to its default directory when the process
        // is confined by a sandbox, and a renderer that cannot record a
        // crash aborts the whole browser. Redirect the reports next to the
        // profile and stop collecting them.
        "--disable-breakpad".into(),
        "--disable-crash-reporter".into(),
        "--no-crashpad".into(),
        format!("--crash-dumps-dir={}", crash_dir(opts)).into(),
    ];
    // The whole point: keep the genuine Chrome fingerprint but drop the
    // `HeadlessChrome` token that gives the headless build away.
    if !user_agent.trim().is_empty() {
        args.push(format!("--user-agent={}", user_agent.trim()).into());
    }

    let executable = if opts.executable.as_os_str().is_empty() {
        default_executable()?
    } else {
        opts.executable.clone()
    };
    // Reuse this profile for the process lifetime, not across container lifetimes:
    // Chromium's SingletonLock embeds a hostname that changes on recreation.
    let profile = tempfile::Builder::new()
        .prefix("mint-")
        .tempdir_in(&opts.user_data_dir)
        .map_err(|e| format!("cannot create a browser process profile: {e}"))?;
    let profile_path = std::fs::canonicalize(profile.path())
        .map_err(|e| format!("cannot resolve browser profile: {e}"))?;
    let port_file = profile_path.join("DevToolsActivePort");
    // Keep a warm browser, not a spare renderer or background discovery/hint
    // services. Merge the library's disabled features into one switch: repeated
    // --disable-features switches would overwrite rather than extend each other.
    let disabled_features = DEFAULT_ARGS
        .iter()
        .filter_map(|arg| arg.strip_prefix("--disable-features="))
        .chain([
            "MediaRouter",
            "OptimizationHints",
            "SpareRendererForSitePerProcess",
        ])
        .collect::<Vec<_>>()
        .join(",");
    args.push(format!("--disable-features={disabled_features}").into());
    // Keep the library's automation setup, but restore Chrome's idle throttling.
    let defaults = DEFAULT_ARGS.iter().filter(|arg| {
        !arg.starts_with("--disable-features=")
            && !matches!(
                **arg,
                "--disable-dev-shm-usage"
                    | "--disable-background-timer-throttling"
                    | "--disable-backgrounding-occluded-windows"
                    | "--disable-renderer-backgrounding"
            )
    });
    let mut child = BrowserChild(
        Command::new(executable)
            .args(defaults)
            .args(args)
            .args([
                "--headless=new",
                "--no-sandbox",
                "--window-size=1280,900",
                "--remote-debugging-port=0",
            ])
            .arg(format!("--user-data-dir={}", profile_path.display()))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("cannot launch Chromium ({}): {e}", describe(opts)))?,
    );
    let stderr = child.0.stderr.take().expect("piped Chromium stderr");
    // Block on the pipe while idle, without a polling timer. EOF ends the reader
    // when Chromium exits; do not join it before killing/reaping the child.
    std::thread::spawn(move || forward_browser_stderr(stderr));
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child
            .0
            .try_wait()
            .map_err(|e| format!("cannot inspect Chromium: {e}"))?
        {
            return Err(format!("Chromium exited before opening DevTools: {status}"));
        }
        if let Ok(endpoint) = std::fs::read_to_string(&port_file) {
            let mut lines = endpoint.lines();
            if let (Some(port), Some(path)) = (lines.next(), lines.next()) {
                let debug_ws_url = url::Url::parse(&format!("ws://127.0.0.1:{port}{path}"))
                    .map_err(|e| format!("invalid Chromium endpoint: {e}"))?;
                return Ok(Process {
                    child,
                    debug_ws_url,
                    _profile: profile,
                });
            }
        }
        if Instant::now() >= deadline {
            return Err("Chromium did not open DevTools within 30s".into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The disposable context isolates stale cookies and is also removed on disconnect.
fn mint_with_browser(
    transport: &Transport,
    opts: &BrowserOptions,
    user_agent: &str,
    cancelled: &AtomicBool,
    stopped: &AtomicBool,
) -> Result<MintedCookie, String> {
    let context = transport
        .call_method_on_browser(Target::CreateBrowserContext {
            dispose_on_detach: Some(true),
            proxy_server: None,
            proxy_bypass_list: None,
            origins_with_universal_network_access: None,
        })
        .map_err(|e| format!("cannot create a mint context: {e}"))?
        .browser_context_id;
    let result = mint_in_context(transport, &context, opts, user_agent, cancelled, stopped);
    if let Err(error) = transport.call_method_on_browser(Target::DisposeBrowserContext {
        browser_context_id: context,
    }) {
        // disposeOnDetach is the fallback, including cancellation and failed CDP calls.
        log::warn!("mint context cleanup deferred to disconnect: {error}");
    }
    // Reclaim caches only after the challenge is over, while the mint lock still
    // excludes the next caller. This is a one-shot notification, not persistent
    // pressure or a periodic idle timer. Unsupported CDP versions must not turn
    // a successfully minted cookie into an error.
    if let Err(error) = transport.call_method_on_browser(Memory::SimulatePressureNotification {
        level: Memory::PressureLevel::Critical,
    }) {
        log::debug!("Chromium idle memory reclamation unavailable: {error}");
    }
    result
}

fn mint_in_context(
    transport: &Transport,
    context: &str,
    opts: &BrowserOptions,
    user_agent: &str,
    cancelled: &AtomicBool,
    stopped: &AtomicBool,
) -> Result<MintedCookie, String> {
    let target = transport
        .call_method_on_browser(Target::CreateTarget {
            url: "about:blank".into(),
            browser_context_id: Some(context.into()),
            left: None,
            top: None,
            width: None,
            height: None,
            window_state: None,
            enable_begin_frame_control: None,
            new_window: None,
            background: None,
            for_tab: None,
            hidden: None,
        })
        .map_err(|e| format!("cannot open a mint tab: {e}"))?
        .target_id;
    let session = SessionId::from(
        transport
            .call_method_on_browser(Target::AttachToTarget {
                target_id: target.clone(),
                flatten: None,
            })
            .map_err(|e| format!("cannot attach to the mint tab: {e}"))?
            .session_id,
    );
    transport
        .call_method_on_target(
            session.clone(),
            Emulation::SetUserAgentOverride {
                user_agent: user_agent.into(),
                accept_language: None,
                platform: None,
                user_agent_metadata: None,
            },
        )
        .map_err(|e| format!("cannot set browser user agent: {e}"))?;
    log::debug!("waiting for Cloudflare clearance in the mint tab");
    let navigation = transport
        .call_method_on_target(
            session.clone(),
            Page::Navigate {
                url: opts.target_url.clone(),
                referrer: None,
                transition_Type: None,
                frame_id: None,
                referrer_policy: None,
            },
        )
        .map_err(|e| format!("navigation to {} failed: {e}", opts.target_url))?;
    if let Some(error) = navigation.error_text {
        return Err(format!("navigation to {} failed: {error}", opts.target_url));
    }
    let deadline = Instant::now() + opts.nav_timeout;
    loop {
        if cancelled.load(Ordering::Acquire) || stopped.load(Ordering::Acquire) {
            return Err("cookie mint cancelled".into());
        }
        let cookies = transport
            .call_method_on_target(
                session.clone(),
                Network::GetCookies {
                    urls: Some(vec![opts.target_url.clone()]),
                },
            )
            .map_err(|e| format!("cannot read clearance cookies: {e}"))?
            .cookies;
        if let Some(cookie) = cookies
            .into_iter()
            .find(|cookie| cookie.name == "cf_clearance" && !cookie.value.trim().is_empty())
        {
            return Ok(MintedCookie {
                cookie: cookie.value,
                user_agent: user_agent.into(),
            });
        }
        if Instant::now() >= deadline {
            let title = transport
                .call_method_on_browser(Target::GetTargetInfo {
                    target_id: Some(target),
                })
                .map(|info| info.target_info.title)
                .unwrap_or_default();
            return Err(format!(
                "timed out after {}s waiting for the Cloudflare challenge to clear (last page title: {title:?})",
                opts.nav_timeout.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(400));
    }
}

/// A writable directory for Chromium's crash dumps.
///
/// It lives beside the profile so it is covered by the same mount/bind rules,
/// rather than in the platform's user-data area which a sandbox usually denies.
fn crash_dir(opts: &BrowserOptions) -> String {
    opts.user_data_dir
        .join("crash-dumps")
        .to_string_lossy()
        .into_owned()
}

fn describe(opts: &BrowserOptions) -> String {
    if opts.executable.as_os_str().is_empty() {
        "auto-detected Chrome".to_string()
    } else {
        opts.executable.display().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_default_to_auto_detection() {
        let opts = BrowserOptions::new("data/browser", "https://supjav.com/popular", "ua/1");
        assert!(opts.executable.as_os_str().is_empty());
        assert_eq!(opts.user_agent, "ua/1");
        assert!(
            opts.nav_timeout >= Duration::from_secs(60),
            "the challenge occasionally takes far longer than a page load"
        );
    }

    #[test]
    fn minter_reports_not_running_before_launch() {
        let minter = BrowserMinter::new(BrowserOptions::new("data/browser", "https://x/", "ua"));
        assert!(!minter.is_running());
    }

    #[test]
    fn describe_mentions_autodetection_when_no_path_is_set() {
        let opts = BrowserOptions::new("p", "https://x/", "ua");
        assert_eq!(describe(&opts), "auto-detected Chrome");

        let mut explicit = opts.clone();
        explicit.executable = PathBuf::from("/usr/bin/chromium");
        assert_eq!(describe(&explicit), "/usr/bin/chromium");
    }
}
