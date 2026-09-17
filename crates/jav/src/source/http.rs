//! HTTP plumbing: a single `wreq` client configured to emulate Chrome, plus
//! helpers that attach the Cloudflare credentials.
//!
//! `wreq` reproduces Chrome's TLS/JA3 fingerprint, which is what makes a
//! `cf_clearance` cookie usable from a non-browser client. A plain
//! `reqwest`/`curl` request is rejected even with a valid cookie because the
//! fingerprint does not match.
//!
//! The cookie itself lives in a [`CookieStore`], not in the client, so a value
//! minted mid-flight by the headless browser is picked up by the next request
//! without rebuilding anything.

use std::sync::Arc;

use anyhow::Context;
use wreq_util::Profile;

use crate::source::cf::{CookieStore, is_cloudflare_challenge};

/// Chrome emulation profile used for every request.
///
/// This must track the Chromium the browser side actually runs: Cloudflare
/// binds `cf_clearance` to the fingerprint that earned it, so a stale profile
/// is indistinguishable from a bot. Kept in one place so both the client and
/// the browser launcher agree.
pub const EMULATION: Profile = Profile::Chrome149;

/// A browser-like `Accept` header for navigation requests.
const ACCEPT_HTML: &str =
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8";

/// Build the shared client. Chrome emulation is mandatory, not cosmetic.
pub fn build_client() -> anyhow::Result<wreq::Client> {
    wreq::Client::builder()
        .emulation(EMULATION)
        .redirect(wreq::redirect::Policy::limited(10))
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .context("failed to build HTTP client")
}

/// Normalise whatever the user pasted into a bare `cf_clearance` value.
/// Accepts a raw value, `cf_clearance=value`, or a full `Cookie:` header.
pub fn normalize_cf_clearance(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return String::new();
    }
    let value = if let Some(idx) = raw.find("cf_clearance=") {
        let after = &raw[idx + "cf_clearance=".len()..];
        after
            .split([';', ',', '\n', '\r'])
            .next()
            .unwrap_or(after)
            .trim()
    } else {
        raw
    };
    value.trim().trim_matches('"').to_string()
}

/// The `Origin`-ish referer a browser would send for `url`.
pub fn referer_for(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    parsed.host_str()?;
    Some(format!("{}/", parsed.origin().ascii_serialization()))
}

/// Attach the CF cookie only when `target` lives on the same site as the page
/// that issued it — leaking the cookie to a CDN host is both useless and rude.
pub fn apply_cf_headers_scoped(
    req: wreq::RequestBuilder,
    target: &str,
    origin: &str,
    cookie: &str,
    user_agent: &str,
) -> wreq::RequestBuilder {
    let mut req = req;
    let ua = user_agent.trim();
    if !ua.is_empty() {
        req = req.header("user-agent", ua);
    }
    if !same_site(target, origin) {
        return req;
    }
    let cf = normalize_cf_clearance(cookie);
    if cf.is_empty() {
        return req;
    }
    req.header("cookie", format!("cf_clearance={cf}"))
}

/// Loose same-site test: equal hosts, or one is a subdomain of the other.
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

/// A request factory bundling the client with the site credentials.
///
/// Cheap to clone (the underlying `wreq::Client` is an `Arc` internally), so
/// it can be handed to every download task. The cookie is read from the shared
/// [`CookieStore`] per request rather than captured, so a refresh propagates to
/// tasks that are already running.
#[derive(Clone)]
pub struct Fetcher {
    pub client: wreq::Client,
    pub cookies: Arc<CookieStore>,
    site_base: String,
}

impl Fetcher {
    pub fn new(client: wreq::Client, cookies: Arc<CookieStore>, site_base: String) -> Self {
        Self {
            client,
            cookies,
            site_base,
        }
    }

    /// Ask the store for a fresh cookie. `force` skips the cooldown.
    pub async fn refresh_cookie(&self, reason: &str, force: bool) -> Result<(), String> {
        self.cookies.refresh(reason, force).await
    }

    /// A GET request with the browser-ish headers the site expects.
    fn request(
        &self,
        url: &str,
        referer: Option<&str>,
        credentials: &(String, String),
    ) -> wreq::RequestBuilder {
        let mut req = self.client.get(url).header("accept", ACCEPT_HTML);
        if let Some(referer) = referer {
            req = req.header("referer", referer);
        }
        apply_cf_headers_scoped(req, url, &self.site_base, &credentials.0, &credentials.1)
    }

    /// GET media, using the page origin as referer and scoping credentials
    /// to the configured site that issued them.
    pub fn media_request(
        &self,
        url: &str,
        origin: &str,
        range: Option<(u64, u64)>,
    ) -> wreq::RequestBuilder {
        let mut req = self.client.get(url);
        if let Some(referer) = referer_for(origin) {
            req = req.header("referer", referer);
        }
        let (cookie, user_agent) = self.cookies.credentials();
        req = apply_cf_headers_scoped(req, url, &self.site_base, &cookie, &user_agent);
        if let Some((start, len)) = range {
            let end = start + len.saturating_sub(1);
            req = req.header("range", format!("bytes={start}-{end}"));
        }
        req
    }

    /// Reject access-denied and HTML responses before they become media files.
    /// The site's clearance cannot authorize a separate CDN such as Surrit.
    pub async fn media_response(
        &self,
        url: &str,
        origin: &str,
        range: Option<(u64, u64)>,
    ) -> anyhow::Result<wreq::Response> {
        let resp = self.media_request(url, origin, range).send().await?;
        let status = resp.status();
        let headers = resp.headers();
        let challenge = headers
            .get("cf-mitigated")
            .is_some_and(|v| v == "challenge");
        let html = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.to_ascii_lowercase().contains("text/html"));
        let denied = status.is_client_error()
            && status != wreq::StatusCode::REQUEST_TIMEOUT
            && status != wreq::StatusCode::TOO_MANY_REQUESTS;
        if challenge || html && status.is_success() || denied {
            let ray = headers.get("cf-ray").and_then(|v| v.to_str().ok());
            let reason = if challenge {
                "Cloudflare challenge"
            } else if ray.is_some() && status == wreq::StatusCode::FORBIDDEN {
                "Cloudflare access denied"
            } else if html && status.is_success() {
                "HTML instead of media"
            } else {
                "access failed"
            };
            return Err(MediaRejected(format!(
                "media request for {}: {reason} (HTTP {status}, cf-ray: {}). Clearance cookies are scoped to the issuing site",
                resp.uri(), ray.unwrap_or("unavailable")
            )).into());
        }
        if !status.is_success() {
            anyhow::bail!("media request for {}: HTTP {status}", resp.uri());
        }
        Ok(resp)
    }

    /// GET a URL and decode the body as text.
    ///
    /// If Cloudflare answers with a challenge (or a `403`), one fresh cookie is
    /// minted and the request is retried once. The retry is what makes an
    /// expired cookie self-healing instead of a hard failure.
    pub async fn text(&self, url: &str, referer: Option<&str>) -> anyhow::Result<String> {
        let attempted_credentials = self.cookies.credentials();
        match self.text_once(url, referer, &attempted_credentials).await {
            Err(e) => {
                let Some(signal) = ChallengeSignal::from_error(&e) else {
                    return Err(e);
                };
                if !same_site(url, &self.site_base) {
                    return Err(e.context("the configured site cookie cannot clear another host"));
                }
                if let Err(mint_err) = self
                    .cookies
                    .refresh_rejected(&signal.reason, attempted_credentials)
                    .await
                {
                    return Err(e.context(format!("automatic cookie refresh failed: {mint_err}")));
                }
                self.text_once(url, referer, &self.cookies.credentials())
                    .await
            }
            ok => ok,
        }
    }

    /// One attempt. [`ChallengeSignal`] marks the "retry with a fresh cookie"
    /// cases so [`Fetcher::text`] can tell them apart from real failures.
    async fn text_once(
        &self,
        url: &str,
        referer: Option<&str>,
        credentials: &(String, String),
    ) -> anyhow::Result<String> {
        let resp = self.request(url, referer, credentials).send().await?;
        let status = resp.status();
        let body = resp.text().await?;

        if is_cloudflare_challenge(&body) {
            return Err(anyhow::Error::new(ChallengeSignal::body(url)));
        }
        if status == wreq::StatusCode::FORBIDDEN {
            return Err(anyhow::Error::new(ChallengeSignal::status(url)));
        }
        if !status.is_success() {
            anyhow::bail!("HTTP {status} for {url}");
        }
        Ok(body)
    }
}

/// A permanent media rejection: retrying every segment cannot repair it.
#[derive(Debug)]
pub struct MediaRejected(String);

impl std::fmt::Display for MediaRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MediaRejected {}

/// Some block pages arrive with HTTP 200 and an incorrect content type.
pub fn validate_media_body(url: &str, data: &[u8]) -> anyhow::Result<()> {
    let prefix = String::from_utf8_lossy(&data[..data.len().min(8192)]);
    let lower = prefix.trim_start().to_ascii_lowercase();
    if data.is_empty()
        || is_cloudflare_challenge(&prefix)
        || lower.starts_with("<!doctype html")
        || lower.starts_with("<html")
        || lower.contains("<title>attention required")
    {
        return Err(MediaRejected(format!(
            "media request for {url}: empty, HTML or Cloudflare response instead of media"
        ))
        .into());
    }
    Ok(())
}

/// Marker error used to distinguish "needs a fresh cookie" from a real failure.
#[derive(Debug)]
struct ChallengeSignal {
    reason: String,
}

impl ChallengeSignal {
    fn body(url: &str) -> Self {
        Self {
            reason: format!("challenge page for {url}"),
        }
    }

    fn status(url: &str) -> Self {
        Self {
            reason: format!("HTTP 403 for {url}"),
        }
    }

    fn from_error(err: &anyhow::Error) -> Option<Self> {
        err.downcast_ref::<ChallengeSignal>().map(|s| Self {
            reason: s.reason.clone(),
        })
    }
}

impl std::fmt::Display for ChallengeSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cloudflare {}", self.reason)
    }
}

impl std::error::Error for ChallengeSignal {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_raw_value() {
        assert_eq!(normalize_cf_clearance("  abc123  "), "abc123");
    }

    #[test]
    fn normalizes_full_cookie_string() {
        assert_eq!(
            normalize_cf_clearance("cf_clearance=abc123; other=1"),
            "abc123"
        );
        assert_eq!(
            normalize_cf_clearance("_cfuvid=x; cf_clearance=abc.def-1; z=2"),
            "abc.def-1"
        );
    }

    #[test]
    fn strips_quotes() {
        assert_eq!(normalize_cf_clearance("\"abc==\""), "abc==");
    }

    #[test]
    fn same_site_matches_subdomains_only() {
        assert!(same_site("https://missav.ai/a", "https://missav.ai/b"));
        assert!(same_site("https://img.missav.ai/a", "https://missav.ai/b"));
        assert!(!same_site("https://evil.com/a", "https://missav.ai/b"));
    }

    #[test]
    fn referer_uses_origin() {
        assert_eq!(
            referer_for("https://supjav.com/457980.html").as_deref(),
            Some("https://supjav.com/")
        );
    }

    #[test]
    fn detects_challenge_page() {
        assert!(is_cloudflare_challenge("<title>Just a moment...</title>"));
        assert!(!is_cloudflare_challenge("<title>Popular</title>"));
    }

    #[test]
    fn challenge_signal_survives_the_error_chain() {
        let sig = ChallengeSignal::status("https://supjav.com/popular");
        let err = anyhow::Error::new(sig).context("wrapped");
        let found = ChallengeSignal::from_error(&err).expect("signal should be found");
        assert!(found.reason.contains("403"));
    }

    #[test]
    fn plain_errors_are_not_challenge_signals() {
        let err = anyhow::anyhow!("connection reset by peer");
        assert!(ChallengeSignal::from_error(&err).is_none());
    }

    /// The fallback user agent and the TLS profile must name the same browser.
    /// A UA that disagrees with the handshake is exactly the tell this whole
    /// design exists to remove, so guard it here.
    #[test]
    fn default_user_agent_agrees_with_the_tls_profile() {
        let ua = crate::source::cf::DEFAULT_USER_AGENT;
        let major = match EMULATION {
            Profile::Chrome149 => 149,
            other => panic!("update DEFAULT_USER_AGENT for the new profile {other:?}"),
        };
        assert!(
            ua.contains(&format!("Chrome/{major}.")),
            "DEFAULT_USER_AGENT {ua:?} does not match the {EMULATION:?} profile (Chrome {major})"
        );
        assert!(
            !ua.contains("Firefox"),
            "a Firefox UA over a Chrome TLS handshake is a bot tell"
        );
    }

    #[test]
    fn challenge_cookie_is_only_sent_to_the_issuing_site() {
        let scoped = apply_cf_headers_scoped(
            wreq::Client::new().get("https://cdn.example/x"),
            "https://cdn.example/x",
            "https://supjav.com/p",
            "cookie-value",
            "ua",
        )
        .build()
        .expect("request builds");
        assert!(
            scoped.headers().get("cookie").is_none(),
            "the clearance cookie must not leak to a third-party CDN"
        );
    }
}
