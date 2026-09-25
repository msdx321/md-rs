//! HTTP plumbing for 91Porn: one Chrome-emulating client plus a shared cookie
//! jar, with the configured account cookie seeded into it.
//!
//! The session matters for correctness, not just for signing in. The site hands
//! out a `CLIPSHARE` cookie on the first *listing* request and serves the video
//! actually named by `?viewkey=` only to requests that carry it; without the
//! cookie every video page returns a different random video. So requests must
//! share a cookie jar and the listing must be visited once per session before
//! any video page is resolved.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use wreq_util::Profile;

/// Chrome emulation profile used for every request.
pub const EMULATION: Profile = Profile::Chrome149;

/// Fallback user agent. Must name the same browser as [`EMULATION`].
pub const DEFAULT_USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
     AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36";

/// A browser-like `Accept` header for navigation requests.
const ACCEPT_HTML: &str =
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8";

/// Build the shared transport. Each request supplies its session generation's jar.
pub fn build_client() -> anyhow::Result<wreq::Client> {
    wreq::Client::builder()
        .emulation(EMULATION)
        .redirect(wreq::redirect::Policy::limited(10))
        .connect_timeout(Duration::from_secs(60))
        .build()
        .map_err(|error| anyhow::anyhow!("failed to build HTTP client: {error}"))
}

/// The site origin a page belongs to, used as the `Referer` for its media.
pub fn origin_of(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    parsed.host_str()?;
    Some(format!("{}/", parsed.origin().ascii_serialization()))
}

/// A request factory bundling the client with the site credentials.
///
/// A fetcher binds one operation to a cookie generation. Responses from retired
/// operations can only mutate their retired jar, never replacement credentials.
#[derive(Clone)]
pub struct Fetcher {
    client: wreq::Client,
    user_agent: String,
    session: Session,
    generation: Arc<SessionGeneration>,
}

impl Fetcher {
    pub fn new(client: wreq::Client, user_agent: String, session: &Session) -> Self {
        Self {
            client,
            user_agent,
            session: session.clone(),
            generation: session
                .current
                .lock()
                .expect("session lock poisoned")
                .clone(),
        }
    }

    pub(crate) fn check_current(&self) -> anyhow::Result<()> {
        let current = self.session.current.lock().expect("session lock poisoned");
        anyhow::ensure!(
            Arc::ptr_eq(&current, &self.generation),
            "91Porn session changed; retry the operation"
        );
        Ok(())
    }

    fn apply(&self, mut req: wreq::RequestBuilder) -> anyhow::Result<wreq::RequestBuilder> {
        self.check_current()?;
        req = req.cookie_provider(self.generation.jar.clone());
        let ua = self.user_agent.trim();
        if !ua.is_empty() {
            req = req.header("user-agent", ua);
        }
        Ok(req)
    }

    /// GET a URL and decode the body as text.
    pub async fn text(&self, url: &str, referer: Option<&str>) -> anyhow::Result<String> {
        let mut req = self
            .client
            .get(url)
            .header("accept", ACCEPT_HTML)
            .timeout(Duration::from_secs(60));
        if let Some(referer) = referer {
            req = req.header("referer", referer);
        }
        let resp = self.apply(req)?.send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        self.check_current()?;
        if !status.is_success() {
            anyhow::bail!("HTTP {status} for {url}");
        }
        anyhow::ensure!(
            !body.trim().is_empty(),
            "empty response for {url} (HTTP {status})"
        );
        Ok(body)
    }

    /// GET media, optionally a byte range, rejecting HTML error pages.
    ///
    /// `range` is `(start, end)`; a `None` end means "to the end of the file",
    /// which is what a resumed download asks for.
    pub async fn media_response(
        &self,
        url: &str,
        origin: &str,
        range: Option<(u64, Option<u64>)>,
    ) -> anyhow::Result<wreq::Response> {
        let mut req = self.client.get(url);
        if let Some(referer) = origin_of(origin) {
            req = req.header("referer", referer);
        }
        req = self.apply(req)?;
        if let Some((start, end)) = range {
            let header = match end {
                Some(end) => format!("bytes={start}-{end}"),
                None => format!("bytes={start}-"),
            };
            req = req.header("range", header);
        }
        let resp = tokio::time::timeout(Duration::from_secs(60), req.send()).await??;
        self.check_current()?;
        let status = resp.status();
        if !status.is_success() && status != wreq::StatusCode::RANGE_NOT_SATISFIABLE {
            anyhow::bail!("media request for {url}: HTTP {status}");
        }
        let html = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.to_ascii_lowercase().contains("text/html"));
        anyhow::ensure!(
            !html || status == wreq::StatusCode::RANGE_NOT_SATISFIABLE,
            "media request for {url}: HTML instead of media"
        );
        Ok(resp)
    }
}

/// The cookie jar plus the "has the listing been visited yet" flag.
///
/// The site's session cookie is what makes `?viewkey=` mean anything, so a
/// cold session has to be warmed by one listing request before videos resolve.
#[derive(Clone)]
pub struct Session {
    current: Arc<Mutex<Arc<SessionGeneration>>>,
}

struct SessionGeneration {
    jar: Arc<wreq::cookie::Jar>,
    warmed: AtomicBool,
    warm: tokio::sync::Mutex<()>,
}

impl Default for SessionGeneration {
    fn default() -> Self {
        Self {
            jar: Arc::new(wreq::cookie::Jar::default()),
            warmed: AtomicBool::new(false),
            warm: tokio::sync::Mutex::new(()),
        }
    }
}

impl Session {
    pub fn new() -> Self {
        Self {
            current: Arc::new(Mutex::new(Arc::new(SessionGeneration::default()))),
        }
    }

    /// Replace credentials atomically. Never clear/reuse a jar still owned by
    /// an in-flight request: its late Set-Cookie would resurrect the old session.
    pub fn set_configured_cookies(&self, cookie: Option<&str>, site_base: &str) {
        let generation = SessionGeneration::default();
        if let Some(cookie) = cookie {
            let cookie = cookie
                .trim()
                .strip_prefix("Cookie:")
                .or_else(|| cookie.trim().strip_prefix("cookie:"))
                .unwrap_or_else(|| cookie.trim())
                .trim();
            let site = site_base.trim();
            if !site.is_empty() {
                for pair in cookie.split([';', '\n', '\r']) {
                    let pair = pair.trim();
                    if !pair.is_empty() && pair.contains('=') {
                        generation.jar.add(format!("{pair}; Path=/"), site);
                    }
                }
            }
        }
        *self.current.lock().expect("session lock poisoned") = Arc::new(generation);
    }

    /// True once the current cookie generation has established a site session.
    pub fn is_warm(&self) -> bool {
        self.current
            .lock()
            .expect("session lock poisoned")
            .warmed
            .load(Ordering::Acquire)
    }

    /// Only the generation that actually performed the listing can become warm.
    pub fn mark_warm(&self, fetch: &Fetcher) -> anyhow::Result<()> {
        let current = self.current.lock().expect("session lock poisoned");
        anyhow::ensure!(
            Arc::ptr_eq(&current, &fetch.generation),
            "91Porn session changed; retry the operation"
        );
        current.warmed.store(true, Ordering::Release);
        Ok(())
    }

    /// A stale resolver must not invalidate a replacement session.
    pub fn reset(&self, fetch: &Fetcher) -> anyhow::Result<()> {
        let current = self.current.lock().expect("session lock poisoned");
        anyhow::ensure!(
            Arc::ptr_eq(&current, &fetch.generation),
            "91Porn session changed; retry the operation"
        );
        current.warmed.store(false, Ordering::Release);
        Ok(())
    }

    /// Concurrent callers within one generation share a single warm-up request.
    /// New credentials do not wait for retired network requests to finish.
    pub async fn ensure(
        &self,
        listing_url: &str,
        referer: &str,
        fetch: &Fetcher,
    ) -> anyhow::Result<()> {
        fetch.check_current()?;
        let generation = &fetch.generation;
        let _guard = generation.warm.lock().await;
        fetch.check_current()?;
        if generation.warmed.load(Ordering::Acquire) {
            return Ok(());
        }
        log::debug!("warming the 91Porn session via a listing request");
        fetch.text(listing_url, Some(referer)).await?;
        self.mark_warm(fetch)
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

/// Loose same-site test: equal hosts, or one is a subdomain of the other.
#[cfg(test)]
fn same_site(a: &str, b: &str) -> bool {
    let (Ok(a), Ok(b)) = (url::Url::parse(a), url::Url::parse(b)) else {
        return false;
    };
    let (Some(a), Some(b)) = (a.host_str(), b.host_str()) else {
        return false;
    };
    let a = a.trim_start_matches("www.").to_lowercase();
    let b = b.trim_start_matches("www.").to_lowercase();
    a == b || a.ends_with(&format!(".{b}")) || b.ends_with(&format!(".{a}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Session {
        fn jar(&self) -> Arc<wreq::cookie::Jar> {
            self.current.lock().unwrap().jar.clone()
        }
    }

    fn fetcher(session: &Session) -> Fetcher {
        Fetcher::new(
            wreq::Client::builder()
                .no_proxy()
                .redirect(wreq::redirect::Policy::limited(10))
                .build()
                .unwrap(),
            String::new(),
            session,
        )
    }

    #[tokio::test]
    async fn obsolete_warmup_cannot_populate_or_warm_reconfigured_cookie_session() {
        use crate::test_support::http::{Server, response};
        // Exercise both replacing an account and removing its credentials.
        for replacement in [Some("account=new"), None] {
            let mut server = Server::new().await;
            let session = Session::new();
            session.set_configured_cookies(Some("account=old"), &server.url);
            let old = fetcher(&session);
            let old_jar = session.jar();
            let owner = session.clone();
            let fetch = old.clone();
            let url = server.url.clone();
            let warming = tokio::spawn(async move { owner.ensure(&url, &url, &fetch).await });
            let held = server.next().await;
            assert!(held.head.contains("account=old"));

            session.set_configured_cookies(replacement, &server.url);
            assert!(!session.is_warm());
            let new = fetcher(&session);
            let new_jar = session.jar();
            assert!(!Arc::ptr_eq(&old_jar, &new_jar));
            assert!(session.mark_warm(&old).is_err());
            assert!(session.reset(&old).is_err());
            let owner = session.clone();
            let fetch = new.clone();
            let url = server.url.clone();
            let current = tokio::spawn(async move { owner.ensure(&url, &url, &fetch).await });
            let request = server.next().await;
            assert!(!request.head.contains("account=old"));
            assert_eq!(request.head.contains("account=new"), replacement.is_some());
            drop(request.respond(response(
                "200 OK",
                "Set-Cookie: CLIPSHARE=new; Path=/\r\n",
                b"new listing",
            )));
            current.await.unwrap().unwrap();
            assert!(
                session.is_warm(),
                "new session warms without waiting for old request"
            );

            // A late redirect also keeps its Set-Cookie and follow-up cookies in
            // the retired jar; the replacement session is never its provider.
            drop(held.respond(response(
                "302 Found",
                "Location: /obsolete\r\nSet-Cookie: CLIPSHARE=old; Path=/\r\n",
                b"",
            )));
            let redirected = server.next().await;
            assert!(redirected.head.contains("CLIPSHARE=old"));
            assert!(redirected.head.contains("account=old"));
            assert!(!redirected.head.contains("account=new"));
            drop(redirected.respond(response(
                "200 OK",
                "Set-Cookie: account=resurrected; Path=/\r\n",
                b"old listing",
            )));
            assert!(
                warming
                    .await
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("session changed")
            );
            assert_eq!(
                old_jar.get("account", &server.url).unwrap().value(),
                "resurrected"
            );
            assert_eq!(
                new_jar.get("CLIPSHARE", &server.url).unwrap().value(),
                "new"
            );
            assert_eq!(
                new_jar
                    .get("account", &server.url)
                    .map(|c| c.value().to_string()),
                replacement.map(|_| "new".into())
            );
            assert!(session.is_warm());
            assert!(session.reset(&old).is_err());
            assert!(session.is_warm());
            assert!(old.text(&server.url, None).await.is_err());
            assert!(
                session
                    .ensure(&server.url, &server.url, &old)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn completed_listing_cannot_mark_a_replacement_generation_warm() {
        use crate::test_support::http::{Server, response};
        let mut server = Server::new().await;
        let session = Session::new();
        let old = fetcher(&session);
        let fetch = old.clone();
        let url = server.url.clone();
        let listing = tokio::spawn(async move { fetch.text(&url, None).await });
        drop(
            server
                .next()
                .await
                .respond(response("200 OK", "", b"listing")),
        );
        listing.await.unwrap().unwrap();
        session.set_configured_cookies(Some("account=new"), &server.url);
        assert!(session.mark_warm(&old).is_err());
        assert!(!session.is_warm());
        let new = fetcher(&session);
        session.mark_warm(&new).unwrap();
        session.reset(&new).unwrap();
        assert!(!session.is_warm());
    }

    #[tokio::test]
    async fn concurrent_warmers_share_one_request_and_failures_remain_cold() {
        use crate::test_support::http::{Server, response};
        let mut server = Server::new().await;
        let session = Session::new();
        let fetch = fetcher(&session);
        let owner = session.clone();
        let failed = fetch.clone();
        let url = server.url.clone();
        let run = tokio::spawn(async move { owner.ensure(&url, &url, &failed).await });
        drop(
            server
                .next()
                .await
                .respond(response("500 Internal Server Error", "", b"error")),
        );
        assert!(run.await.unwrap().is_err());
        assert!(!session.is_warm());

        let mut jobs = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let owner = session.clone();
            let fetch = fetch.clone();
            let url = server.url.clone();
            jobs.spawn(async move { owner.ensure(&url, &url, &fetch).await });
        }
        drop(server.next().await.respond(response(
            "200 OK",
            "Set-Cookie: CLIPSHARE=current; Path=/\r\n",
            b"listing",
        )));
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(result) = jobs.join_next().await {
                result.unwrap().unwrap();
            }
        })
        .await
        .expect("duplicate warm-up request was sent");
        assert!(session.is_warm());
        assert_eq!(
            session.jar().get("CLIPSHARE", &server.url).unwrap().value(),
            "current"
        );
    }

    #[test]
    fn same_site_matches_subdomains_only() {
        assert!(same_site(
            "https://www.91porn.com/a",
            "https://www.91porn.com/b"
        ));
        assert!(!same_site("https://evil.com/a", "https://www.91porn.com/b"));
    }

    #[test]
    fn referer_uses_origin() {
        assert_eq!(
            origin_of("https://www.91porn.com/view_video.php?viewkey=1").as_deref(),
            Some("https://www.91porn.com/")
        );
    }

    #[test]
    fn configured_cookies_land_in_the_jar() {
        let session = Session::new();
        assert!(!session.is_warm());
        session.mark_warm(&fetcher(&session)).unwrap();
        assert!(session.is_warm());
        session.set_configured_cookies(Some("PHPSESSID=abc; userid=7"), "https://www.91porn.com");
        assert!(!session.is_warm(), "new credentials invalidate the session");
        let jar = session.jar();
        let cookie = jar.get("PHPSESSID", "https://www.91porn.com/");
        assert_eq!(
            cookie.map(|c| c.value().to_string()).as_deref(),
            Some("abc")
        );
        assert!(jar.get("PHPSESSID", "https://evil.example/").is_none());
    }

    #[test]
    fn accepts_a_full_cookie_header() {
        let session = Session::new();
        session.set_configured_cookies(Some("Cookie: a=1\nb=2"), "https://www.91porn.com");
        let jar = session.jar();
        assert!(jar.get("a", "https://www.91porn.com/").is_some());
        assert!(jar.get("b", "https://www.91porn.com/").is_some());
    }

    #[test]
    fn default_user_agent_agrees_with_the_tls_profile() {
        let major = match EMULATION {
            Profile::Chrome149 => 149,
            other => panic!("update DEFAULT_USER_AGENT for the new profile {other:?}"),
        };
        assert!(
            DEFAULT_USER_AGENT.contains(&format!("Chrome/{major}.")),
            "DEFAULT_USER_AGENT {DEFAULT_USER_AGENT:?} does not match {EMULATION:?}"
        );
    }
}
