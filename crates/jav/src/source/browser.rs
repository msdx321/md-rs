//! Headless-browser cookie minting.
//!
//! Cloudflare's managed challenge runs a JavaScript VM and a pile of browser
//! fingerprint checks, so no HTTP client can answer it. The only reliable way
//! to obtain a `cf_clearance` cookie is to let a real browser load the page,
//! then lift the cookie out of it and hand it to the fast `wreq` client.
//!
//! Only cookie *minting* goes through the browser; page and segment downloads
//! stay on the much faster `wreq` path. Every mint launches a new browser after
//! closing the previous browser and deleting its profile.
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

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use headless_chrome::browser::tab::Tab;
use headless_chrome::protocol::cdp::Network::Cookie;
use headless_chrome::{Browser, LaunchOptions};

use crate::source::cf::MintedCookie;

/// Tunables for the minting browser.
#[derive(Debug, Clone)]
pub struct BrowserOptions {
    /// Explicit Chromium/Chrome binary. Empty lets the crate auto-detect one.
    pub executable: PathBuf,
    /// Profile directory, deleted and recreated before every mint.
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
/// Exclusivity is already guaranteed by the [`crate::source::cf::CookieStore`] gate, so
/// only one mint can be in flight; the mutex exists so the cached browser can be
/// moved onto the blocking thread that drives it.
pub struct BrowserMinter {
    opts: BrowserOptions,
    slot: Arc<Mutex<Option<Browser>>>,
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

    /// A [`crate::source::cf::MintFn`] bound to this minter.
    pub fn mint_fn(self: &Arc<Self>) -> crate::source::cf::MintFn {
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
            let result = mint_blocking(
                Arc::clone(&slot),
                opts,
                user_agent,
                &running,
                &cancelled,
                &stopped,
            );
            if result.is_err() {
                if let Ok(mut browser) = slot.lock() {
                    browser.take();
                }
                running.store(false, Ordering::Release);
            }
            result
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

/// The blocking half of a mint: reset the browser and profile, then drive the page.
fn mint_blocking(
    slot: Arc<Mutex<Option<Browser>>>,
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
    // Drop and reap the previous browser before touching its profile.
    guard.take();
    running.store(false, Ordering::Release);
    match std::fs::remove_dir_all(&opts.user_data_dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(format!(
                "cannot delete browser profile {}: {e}",
                opts.user_data_dir.display()
            ));
        }
    }
    std::fs::create_dir_all(&opts.user_data_dir).map_err(|e| {
        format!(
            "cannot create browser profile {}: {e}",
            opts.user_data_dir.display()
        )
    })?;

    log::info!("launching the cookie-minting browser ({})", describe(&opts));
    let mut args: Vec<std::ffi::OsString> = vec![
        "--disable-gpu".into(),
        "--disable-dev-shm-usage".into(),
        "--no-first-run".into(),
        "--no-default-browser-check".into(),
        "--disable-background-networking".into(),
        "--disable-blink-features=AutomationControlled".into(),
        // Crashpad cannot write to its default directory when the process
        // is confined by a sandbox, and a renderer that cannot record a
        // crash aborts the whole browser. Redirect the reports next to the
        // profile and stop collecting them.
        "--disable-breakpad".into(),
        "--disable-crash-reporter".into(),
        "--no-crashpad".into(),
        format!("--crash-dumps-dir={}", crash_dir(&opts)).into(),
    ];
    // The whole point: keep the genuine Chrome fingerprint but drop the
    // `HeadlessChrome` token that gives the headless build away.
    if !user_agent.trim().is_empty() {
        args.push(format!("--user-agent={}", user_agent.trim()).into());
    }

    let options = LaunchOptions::default_builder()
        .path(if opts.executable.as_os_str().is_empty() {
            None
        } else {
            Some(opts.executable.clone())
        })
        .user_data_dir(Some(opts.user_data_dir.clone()))
        .headless(true)
        .sandbox(false)
        .window_size(Some((1280, 900)))
        .args(args.iter().map(|a| a.as_os_str()).collect())
        .build()
        .map_err(|e| format!("invalid browser configuration: {e}"))?;

    let browser = Browser::new(options)
        .map_err(|e| format!("cannot launch Chromium ({}): {e}", describe(&opts)))?;
    *guard = Some(browser);
    running.store(true, Ordering::Release);

    let browser = guard.as_ref().expect("just ensured");
    let started = Instant::now();
    let minted = mint_with_browser(browser, &opts, &user_agent, cancelled, stopped)?;
    log::info!(
        "cf_clearance minted in {:.1}s via {}",
        started.elapsed().as_secs_f64(),
        describe(&opts)
    );
    Ok(minted)
}

/// Drive a tab until Cloudflare clears, then return the clearance cookie.
fn mint_with_browser(
    browser: &Browser,
    opts: &BrowserOptions,
    user_agent: &str,
    cancelled: &AtomicBool,
    stopped: &AtomicBool,
) -> Result<MintedCookie, String> {
    let tab = browser
        .new_tab()
        .map_err(|e| format!("cannot open a browser tab: {e}"))?;
    tab.set_default_timeout(opts.nav_timeout);

    tab.set_user_agent(user_agent, None, None)
        .map_err(|e| format!("cannot set browser user agent: {e}"))?;
    log::info!("solving the Cloudflare challenge at {}", opts.target_url);
    tab.navigate_to(&opts.target_url)
        .map_err(|e| format!("navigation to {} failed: {e}", opts.target_url))?;

    let cookie = wait_for_clearance(&tab, opts, cancelled, stopped)?;
    let actual_ua = browser_user_agent(&tab).unwrap_or_else(|| user_agent.to_string());
    let _ = tab.close(true);

    Ok(MintedCookie {
        cookie,
        user_agent: actual_ua,
    })
}

/// Poll the live tab until the clearance cookie lands.
///
/// The cookie is the only condition that matters: it already authorises the
/// HTTP client even if the DOM has not finished swapping in the real page.
fn wait_for_clearance(
    tab: &Tab,
    opts: &BrowserOptions,
    cancelled: &AtomicBool,
    stopped: &AtomicBool,
) -> Result<String, String> {
    let deadline = Instant::now() + opts.nav_timeout;
    let mut last_title = String::new();

    loop {
        if cancelled.load(Ordering::Acquire) || stopped.load(Ordering::Acquire) {
            return Err("cookie mint cancelled".into());
        }
        if let Ok(title) = tab.get_title() {
            last_title = title;
        }
        if let Some(clearance) = tab
            .get_cookies()
            .ok()
            .and_then(|cookies| clearance_value(&cookies))
        {
            return Ok(clearance);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out after {}s waiting for the Cloudflare challenge to clear \
                 (last page title: {last_title:?})",
                opts.nav_timeout.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(400));
    }
}

/// The user agent the page actually reported, if it can be read.
fn browser_user_agent(tab: &Tab) -> Option<String> {
    tab.evaluate("navigator.userAgent", false)
        .ok()
        .and_then(|v| v.value)
        .and_then(|v| v.as_str().map(str::to_string))
        .filter(|ua| !ua.trim().is_empty())
}

/// The `cf_clearance` value in a CDP cookie list, if present.
fn clearance_value(cookies: &[Cookie]) -> Option<String> {
    cookies
        .iter()
        .find(|c| c.name == "cf_clearance")
        .map(|c| c.value.clone())
        .filter(|v| !v.trim().is_empty())
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
