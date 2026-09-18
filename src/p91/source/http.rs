//! HTTP plumbing for 91Porn: one Chrome-emulating client plus a shared cookie
//! jar, with the configured account cookie seeded into it.
//!
//! The session matters for correctness, not just for signing in. The site hands
//! out a `CLIPSHARE` cookie on the first *listing* request and serves the video
//! actually named by `?viewkey=` only to requests that carry it; without the
//! cookie every video page returns a different random video. So requests must
//! share a cookie jar and the listing must be visited once per session before
//! any video page is resolved.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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

/// Build the shared client over the session's cookie jar.
pub fn build_client(jar: Arc<wreq::cookie::Jar>) -> anyhow::Result<wreq::Client> {
    wreq::Client::builder()
        .emulation(EMULATION)
        .cookie_provider(jar)
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
/// Cheap to clone: the client and jar are shared behind the scenes, so a
/// request made by a long-running download sees cookies set in the meantime.
#[derive(Clone)]
pub struct Fetcher {
    client: wreq::Client,
    user_agent: String,
}

impl Fetcher {
    pub fn new(client: wreq::Client, user_agent: String) -> Self {
        Self { client, user_agent }
    }

    fn apply(&self, mut req: wreq::RequestBuilder) -> wreq::RequestBuilder {
        let ua = self.user_agent.trim();
        if !ua.is_empty() {
            req = req.header("user-agent", ua);
        }
        req
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
        let resp = self.apply(req).send().await?;
        let status = resp.status();
        let body = resp.text().await?;
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
        req = self.apply(req);
        if let Some((start, end)) = range {
            let header = match end {
                Some(end) => format!("bytes={start}-{end}"),
                None => format!("bytes={start}-"),
            };
            req = req.header("range", header);
        }
        let resp = tokio::time::timeout(Duration::from_secs(60), req.send()).await??;
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
pub struct Session {
    jar: Arc<wreq::cookie::Jar>,
    warmed: AtomicBool,
    warm: tokio::sync::Mutex<()>,
}

impl Session {
    pub fn new() -> Self {
        Self {
            jar: Arc::new(wreq::cookie::Jar::default()),
            warmed: AtomicBool::new(false),
            warm: tokio::sync::Mutex::new(()),
        }
    }

    pub fn jar(&self) -> Arc<wreq::cookie::Jar> {
        self.jar.clone()
    }

    /// Seed the configured account cookie (`name=value; …`) for the site.
    ///
    /// These live in the jar rather than a request header so the session
    /// cookie the site sets later is sent alongside them instead of replacing
    /// them.
    pub fn set_configured_cookies(&self, cookie: Option<&str>, site_base: &str) {
        self.jar.clear();
        self.reset();
        let Some(cookie) = cookie else {
            return;
        };
        let cookie = cookie
            .trim()
            .strip_prefix("Cookie:")
            .or_else(|| cookie.trim().strip_prefix("cookie:"))
            .unwrap_or_else(|| cookie.trim())
            .trim();
        let site = site_base.trim();
        if cookie.is_empty() || site.is_empty() {
            return;
        }
        for pair in cookie.split([';', '\n', '\r']) {
            let pair = pair.trim();
            if pair.is_empty() || !pair.contains('=') {
                continue;
            }
            self.jar.add(format!("{pair}; Path=/"), site);
        }
    }

    /// True once a listing request has established the site session.
    pub fn is_warm(&self) -> bool {
        self.warmed.load(Ordering::Acquire)
    }

    /// Record that a listing request has already run this session.
    pub fn mark_warm(&self) {
        self.warmed.store(true, Ordering::Release);
    }

    /// Forget the warm-up so the next resolve visits the listing again.
    pub fn reset(&self) {
        self.warmed.store(false, Ordering::Release);
    }

    /// Visit the listing once so the site starts honouring `?viewkey=`.
    ///
    /// Concurrent callers share a single request; later ones see the flag and
    /// return immediately.
    pub async fn ensure(
        &self,
        listing_url: &str,
        referer: &str,
        fetch: &Fetcher,
    ) -> anyhow::Result<()> {
        if self.warmed.load(Ordering::Acquire) {
            return Ok(());
        }
        let _guard = self.warm.lock().await;
        if self.warmed.load(Ordering::Acquire) {
            return Ok(());
        }
        log::debug!("warming the 91Porn session via {listing_url}");
        fetch.text(listing_url, Some(referer)).await?;
        self.warmed.store(true, Ordering::Release);
        Ok(())
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
        session.mark_warm();
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
