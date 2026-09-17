//! Cloudflare clearance-cookie state.
//!
//! Cloudflare's *managed* challenge cannot be answered by an HTTP client, no
//! matter how convincing its TLS fingerprint is: the interstitial runs
//! JavaScript from `challenge-platform/.../orchestrate/` and only then issues
//! the `cf_clearance` cookie. A real browser therefore has to produce the
//! cookie; [`crate::source::browser`] does that, and this module holds the result.
//!
//! The cookie is bound to the IP **and** to the TLS fingerprint that earned it,
//! so it is mutable shared state: it is replaced whenever a fresh one is
//! minted, and every in-flight request picks up the current value.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// User agent used when the configuration does not name one.
///
/// The browser is the authority on this: it reports its own real user agent
/// after minting the cookie. This fallback only covers requests made *before*
/// the first mint, and it must name the same browser as the TLS fingerprint in
/// [`crate::source::http::EMULATION`] or Cloudflare sees the mismatch immediately.
pub const DEFAULT_USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
     AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36";

/// Failure of a cookie-mint attempt, reported back to whoever needed it.
pub type MintError = String;

/// The mint operation, supplied by the caller so this module stays free of any
/// browser dependency.
pub type MintFn = Arc<
    dyn Fn(
            String,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<MintedCookie, MintError>> + Send>,
        > + Send
        + Sync,
>;

/// A freshly minted clearance cookie plus the user agent that earned it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MintedCookie {
    /// Non-empty bare `cf_clearance` value.
    pub cookie: String,
    /// The browser's own user agent. For the cookie to stay valid the HTTP
    /// client must send exactly this value.
    pub user_agent: String,
}

/// How a cookie came to be, surfaced in the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CookieSource {
    /// Pasted by the user (or restored from `config.yaml`).
    Manual,
    /// Minted by the headless browser without user involvement.
    Browser,
}

impl CookieSource {
    pub fn as_str(self) -> &'static str {
        match self {
            CookieSource::Manual => "manual",
            CookieSource::Browser => "browser",
        }
    }
}

#[derive(Debug, Clone)]
struct CookieState {
    value: String,
    user_agent: String,
    source: CookieSource,
    minted_at: Option<Instant>,
    refreshing: bool,
    last_error: Option<String>,
}

impl CookieState {
    fn empty() -> Self {
        Self {
            value: String::new(),
            user_agent: String::new(),
            source: CookieSource::Manual,
            minted_at: None,
            refreshing: false,
            last_error: None,
        }
    }
}

/// A point-in-time view of the store, for status reporting.
#[derive(Debug, Clone)]
pub struct CookieSnapshot {
    pub configured: bool,
    pub source: CookieSource,
    /// Seconds since the current cookie was minted, when known.
    pub age_secs: Option<u64>,
    pub refreshing: bool,
    pub last_error: Option<String>,
}

/// Serialises mint attempts so a burst of `403`s triggers exactly one browser
/// run instead of one per request.
pub(crate) struct MintGate {
    last_attempt: Option<Instant>,
    last_result: Result<(), MintError>,
}

impl Default for MintGate {
    fn default() -> Self {
        Self {
            last_attempt: None,
            last_result: Ok(()),
        }
    }
}

/// Holds the live `cf_clearance` cookie and knows how to refresh it.
#[derive(Clone)]
pub struct CookieStore {
    inner: Arc<RwLock<CookieState>>,
    changes: tokio::sync::watch::Sender<()>,
    /// A *tokio* mutex so its guard stays `Send` across the mint await.
    pub(crate) gate: Arc<tokio::sync::Mutex<MintGate>>,
    generation: Arc<AtomicU64>,
    mint_fn: Arc<RwLock<Option<MintFn>>>,
    /// How long a single mint attempt may take before it is abandoned.
    mint_timeout: Duration,
    /// Minimum spacing after a failed attempt, so a hard failure does not spin the
    /// browser on every request.
    cooldown: Duration,
}

impl std::fmt::Debug for CookieStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.inner.read().ok();
        f.debug_struct("CookieStore")
            .field(
                "configured",
                &state.as_ref().is_some_and(|s| !s.value.is_empty()),
            )
            .field("source", &state.as_ref().map(|s| s.source))
            .field("can_mint", &self.can_mint())
            .finish()
    }
}

impl CookieStore {
    /// A store seeded with a cookie the user pasted in. `mint_fn` may be `None`
    /// when the browser feature is unavailable, in which case refreshing always
    /// fails and the UI keeps asking for a manual paste.
    pub fn new(
        initial: impl Into<String>,
        user_agent: impl Into<String>,
        mint_fn: Option<MintFn>,
    ) -> Self {
        let mut state = CookieState::empty();
        state.value = initial.into().trim().to_string();
        state.user_agent = user_agent.into().trim().to_string();
        Self {
            inner: Arc::new(RwLock::new(state)),
            changes: tokio::sync::watch::channel(()).0,
            gate: Arc::new(tokio::sync::Mutex::new(MintGate::default())),
            generation: Arc::new(AtomicU64::new(0)),
            mint_fn: Arc::new(RwLock::new(mint_fn)),
            mint_timeout: Duration::from_secs(180),
            cooldown: Duration::from_secs(30),
        }
    }

    /// The current cookie value; empty when none is configured.
    #[cfg(test)]
    pub fn cookie(&self) -> String {
        self.inner
            .read()
            .map(|s| s.value.clone())
            .unwrap_or_default()
    }

    /// The user agent that matches the current cookie.
    ///
    /// An empty stored value means "not overridden": fall back to the profile
    /// default so a request never goes out with a blank or mismatched agent.
    pub fn user_agent(&self) -> String {
        let stored = self
            .inner
            .read()
            .map(|s| s.user_agent.clone())
            .unwrap_or_default();
        if stored.trim().is_empty() {
            DEFAULT_USER_AGENT.to_string()
        } else {
            stored
        }
    }

    pub fn snapshot(&self) -> CookieSnapshot {
        let state = self.inner.read().expect("cookie lock poisoned");
        CookieSnapshot {
            configured: !state.value.is_empty(),
            source: state.source,
            age_secs: state.minted_at.map(|t| t.elapsed().as_secs()),
            refreshing: state.refreshing,
            last_error: state.last_error.clone(),
        }
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<()> {
        self.changes.subscribe()
    }

    /// True when automatic minting is possible at all.
    pub fn can_mint(&self) -> bool {
        self.mint_fn.read().expect("minter lock poisoned").is_some()
    }

    /// Replace the cookie by hand (web UI / config reload). The user agent is
    /// left alone unless a non-empty one is supplied.
    #[cfg(test)]
    pub fn set_manual(&self, cookie: impl Into<String>, user_agent: Option<String>) {
        let mut state = self.inner.write().expect("cookie lock poisoned");
        state.value = cookie.into().trim().to_string();
        state.source = CookieSource::Manual;
        state.minted_at = None;
        if let Some(ua) = user_agent {
            let ua = ua.trim().to_string();
            if !ua.is_empty() {
                state.user_agent = ua;
            }
        }
    }

    /// Set the user agent override; a blank value clears it, restoring the
    /// profile default. Called when the configuration is (re)applied.
    #[cfg(test)]
    pub fn set_user_agent(&self, user_agent: impl Into<String>) {
        let ua = user_agent.into().trim().to_string();
        let mut state = self.inner.write().expect("cookie lock poisoned");
        state.user_agent = ua;
    }

    fn install_minted(&self, minted: MintedCookie) {
        let mut state = self.inner.write().expect("cookie lock poisoned");
        if !minted.cookie.trim().is_empty() {
            state.value = minted.cookie.trim().to_string();
        }
        if !minted.user_agent.trim().is_empty() {
            state.user_agent = minted.user_agent.trim().to_string();
        }
        state.source = CookieSource::Browser;
        state.minted_at = Some(Instant::now());
    }

    /// Restore persisted browser credentials without claiming a new mint time.
    pub(crate) fn restore(&self, cookie: String, user_agent: String) {
        let mut state = self.inner.write().expect("cookie lock poisoned");
        state.value = cookie;
        state.user_agent = user_agent;
        state.source = CookieSource::Browser;
        state.minted_at = None;
    }

    /// Mint a fresh cookie, collapsing concurrent callers into one attempt.
    ///
    /// `force` bypasses failure backoff (the UI's explicit "refresh now" button).
    /// When another task already minted a newer cookie by the time this caller
    /// acquires the gate, that cookie is adopted instead of minting again.
    pub async fn refresh(&self, reason: &str, force: bool) -> Result<(), MintError> {
        self.refresh_shared(reason, force, None).await
    }

    /// Recheck the exact rejected credentials under the refresh gate, including
    /// requests that finished after another caller already installed a cookie.
    pub async fn refresh_rejected(
        &self,
        reason: &str,
        credentials: (String, String),
    ) -> Result<(), MintError> {
        self.refresh_shared(reason, false, Some(credentials)).await
    }

    async fn refresh_shared(
        &self,
        reason: &str,
        force: bool,
        rejected: Option<(String, String)>,
    ) -> Result<(), MintError> {
        let observed = self.generation.load(Ordering::Acquire);
        let store = self.clone();
        let reason = reason.to_string();
        // A disconnected HTTP request must not cancel work shared by other
        // callers. The owned task still has the mint timeout and shutdown flag.
        tokio::spawn(async move {
            store
                .refresh_locked(&reason, force, rejected, observed)
                .await
        })
        .await
        .map_err(|e| format!("cookie refresh task failed: {e}"))?
    }

    async fn refresh_locked(
        &self,
        reason: &str,
        force: bool,
        rejected: Option<(String, String)>,
        observed: u64,
    ) -> Result<(), MintError> {
        let mut gate = self.gate.lock().await;
        if rejected.is_some_and(|credentials| credentials != self.credentials()) {
            return Ok(());
        }
        if self.generation.load(Ordering::Acquire) != observed {
            return gate.last_result.clone();
        }
        // Only failed attempts back off. Return the actual failure to every
        // waiter instead of hiding it behind a misleading cooldown error.
        if !force
            && gate.last_result.is_err()
            && gate
                .last_attempt
                .is_some_and(|last| last.elapsed() < self.cooldown)
        {
            return gate.last_result.clone();
        }
        let mint_fn = self
            .mint_fn
            .read()
            .expect("minter lock poisoned")
            .clone()
            .ok_or_else(|| {
                "automatic browser cookie minting is disabled; enable it or paste a cookie in Settings"
                    .to_string()
            })?;
        {
            let mut state = self.inner.write().expect("cookie lock poisoned");
            state.refreshing = true;
            state.last_error = None;
        }
        let _refresh_status = RefreshStatus(self);
        self.changes.send_replace(());
        log::info!("minting a fresh cf_clearance cookie ({reason})");
        let attempt = tokio::time::timeout(self.mint_timeout, mint_fn(self.user_agent())).await;
        gate.last_attempt = Some(Instant::now());
        let result = match attempt {
            Ok(Ok(minted)) if !minted.cookie.trim().is_empty() => {
                self.install_minted(minted);
                Ok(())
            }
            Ok(Ok(_)) => Err("the browser returned an empty clearance cookie".into()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(format!(
                "cookie refresh timed out after {}s",
                self.mint_timeout.as_secs()
            )),
        };
        self.inner.write().expect("cookie lock poisoned").last_error =
            result.as_ref().err().cloned();
        gate.last_result = result.clone();
        self.generation.fetch_add(1, Ordering::Release);
        result
    }

    /// Called with the refresh gate held so settings cannot race a mint.
    pub(crate) fn reconfigure(
        &self,
        gate: &mut MintGate,
        minter: Option<MintFn>,
        credentials: Option<(String, String)>,
    ) {
        *self.mint_fn.write().expect("minter lock poisoned") = minter;
        if let Some((cookie, user_agent)) = credentials {
            let mut state = self.inner.write().expect("cookie lock poisoned");
            state.value = cookie.trim().to_string();
            state.user_agent = user_agent.trim().to_string();
            state.source = CookieSource::Manual;
            state.minted_at = None;
        }
        gate.last_attempt = None;
        gate.last_result = Ok(());
        self.generation.fetch_add(1, Ordering::Release);
        self.inner.write().expect("cookie lock poisoned").last_error = None;
        self.changes.send_replace(());
    }

    /// Read the cookie and its matching user agent under a single lock.
    pub fn credentials(&self) -> (String, String) {
        let state = self.inner.read().expect("cookie lock poisoned");
        let ua = if state.user_agent.is_empty() {
            DEFAULT_USER_AGENT
        } else {
            &state.user_agent
        };
        (state.value.clone(), ua.to_string())
    }
}

/// Clear the in-progress status even when the async mint is cancelled.
struct RefreshStatus<'a>(&'a CookieStore);

impl Drop for RefreshStatus<'_> {
    fn drop(&mut self) {
        self.0
            .inner
            .write()
            .expect("cookie lock poisoned")
            .refreshing = false;
        self.0.changes.send_replace(());
    }
}

/// True when the body is Cloudflare's interstitial rather than the real page.
///
/// Covers the legacy script challenge and the modern managed/orchestrate
/// interstitial, which share the `Just a moment...` title.
pub fn is_cloudflare_challenge(body: &str) -> bool {
    body.contains("Just a moment...")
        || body.contains("cf-challenge")
        || body.contains("cf_chl_opt")
        || body.contains("challenge-platform")
        || body.contains("_cf_chl_")
        || (body.contains("cloudflare") && body.contains("checking your browser"))
}

/// True when an error message looks like a Cloudflare rejection.
pub fn is_challenge_error(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    lower.contains("cloudflare")
        || lower.contains("403")
        || lower.contains("cf_chl")
        || lower.contains("just a moment")
        || lower.contains("cf-mitigated")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_managed_challenge() {
        assert!(is_cloudflare_challenge(
            "<title>Just a moment...</title><script src=\"/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1?ray=x\">"
        ));
        assert!(is_cloudflare_challenge("window._cf_chl_opt = {}"));
        assert!(!is_cloudflare_challenge("<title>Popular</title>"));
    }

    #[test]
    fn recognises_challenge_errors() {
        assert!(is_challenge_error("HTTP 403 for https://x"));
        assert!(is_challenge_error("blocked by Cloudflare (challenge page)"));
        assert!(!is_challenge_error("connection reset by peer"));
    }

    #[test]
    fn manual_cookie_is_visible_immediately() {
        let store = CookieStore::new("abc", "ua/1", None);
        assert_eq!(store.cookie(), "abc");
        assert_eq!(store.user_agent(), "ua/1");
        let snap = store.snapshot();
        assert!(snap.configured);
        assert_eq!(snap.source, CookieSource::Manual);
        assert!(snap.age_secs.is_none());
    }

    #[test]
    fn manual_update_replaces_value_and_marks_manual() {
        let store = CookieStore::new("old", "ua", None);
        store.set_manual("new", None);
        assert_eq!(store.cookie(), "new");
        assert_eq!(store.user_agent(), "ua", "ua is untouched without a hint");
        store.set_manual("other", Some("ua2".into()));
        assert_eq!(store.user_agent(), "ua2");
    }

    #[test]
    fn empty_user_agent_falls_back_to_the_profile_default() {
        let store = CookieStore::new("abc", "", None);
        assert_eq!(store.user_agent(), DEFAULT_USER_AGENT);
        // An explicit override wins.
        store.set_user_agent("Custom/1.0");
        assert_eq!(store.user_agent(), "Custom/1.0");
        // And setting it back to blank restores the fallback.
        store.set_user_agent("   ");
        assert_eq!(store.user_agent(), DEFAULT_USER_AGENT);
    }

    #[test]
    fn refresh_without_a_minter_is_an_error() {
        let store = CookieStore::new("abc", "ua", None);
        assert!(!store.can_mint());
        let err = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(store.refresh("test", true))
            .unwrap_err();
        assert!(err.contains("browser"), "unexpected error: {err}");
    }

    #[test]
    fn refresh_installs_the_minted_cookie() {
        let mint: MintFn = Arc::new(|_ua| {
            Box::pin(async {
                Ok(MintedCookie {
                    cookie: "fresh".into(),
                    user_agent: "Chrome/149".into(),
                })
            })
        });
        let store = CookieStore::new("stale", "Chrome/120", Some(mint));
        assert!(store.can_mint());
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(store.refresh("test", true))
            .unwrap();
        assert_eq!(store.cookie(), "fresh");
        assert_eq!(store.user_agent(), "Chrome/149");
        let snap = store.snapshot();
        assert_eq!(snap.source, CookieSource::Browser);
        assert!(snap.age_secs.is_some());
    }

    /// A burst of concurrent failures must collapse into one browser run: the
    /// losers adopt the cookie the winner minted rather than launching again.
    #[test]
    fn concurrent_refreshes_mint_only_once() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let mint: MintFn = Arc::new(move |_ua| {
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Yield so the other callers pile up behind the gate.
                tokio::task::yield_now().await;
                Ok(MintedCookie {
                    cookie: "fresh".into(),
                    user_agent: "Chrome/149".into(),
                })
            })
        });
        let store = Arc::new(CookieStore::new("stale", "ua", Some(mint)));

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let a = Arc::clone(&store);
            let b = Arc::clone(&store);
            let c = Arc::clone(&store);
            let (ra, rb, rc) = tokio::join!(
                async move { a.refresh("a", false).await },
                async move { b.refresh("b", false).await },
                async move { c.refresh("c", false).await },
            );
            // Exactly one caller mints; every waiter adopts its result.
            assert!(
                ra.is_ok() && rb.is_ok() && rc.is_ok(),
                "all refresh waiters should succeed"
            );
        });
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(store.cookie(), "fresh");
    }

    #[test]
    fn cooldown_preserves_failure_until_forced_retry() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let mint: MintFn = Arc::new(move |_ua| {
            counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err("challenge failed".into()) })
        });
        let store = CookieStore::new("stale", "ua", Some(mint));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(store.refresh("first", false)).unwrap_err();
        // A failed attempt backs off without hiding the original error.
        let err = rt.block_on(store.refresh("second", false)).unwrap_err();
        assert_eq!(err, "challenge failed");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // A forced refresh ignores the cooldown (used by the UI button).
        assert_eq!(
            rt.block_on(store.refresh("forced", true)).unwrap_err(),
            "challenge failed"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
