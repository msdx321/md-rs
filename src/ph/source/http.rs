//! HTTP plumbing for Pornhub: one Chrome-emulating client, a shared cookie jar,
//! and the optional account login.
//!
//! The account login is a two-step form exchange. The login page carries a
//! one-time `token` (and a `redirect` handle); posting them with the account
//! email and password to `/front/authenticate` yields a JSON result whose
//! `redirect` field means the session cookies are now in the jar. The
//! Recommended feed is personalised from that session.
//!
//! A full Cookie header can be configured instead. It is useful when an account
//! is protected by a CAPTCHA or two-factor step that this HTTP-only client
//! cannot answer.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use wreq_util::Profile;

use crate::ph::source::scraper;

/// Chrome emulation profile used for every request.
pub const EMULATION: Profile = Profile::Chrome149;

/// Fallback user agent. Must name the same browser as [`EMULATION`].
pub const DEFAULT_USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
     AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36";

/// A browser-like `Accept` header for navigation requests.
const ACCEPT_HTML: &str =
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8";

/// The AJAX accept header the site uses for `get_media`.
const ACCEPT_JSON: &str = "application/json, text/javascript, */*; q=0.01";

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
            "Pornhub session changed; retry the operation"
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

    /// Fetch a CDN thumbnail with the site's referrer. Do not follow redirects
    /// away from the CDN URL validated by the API.
    pub async fn thumbnail_response(
        &self,
        url: &str,
        site: &str,
    ) -> anyhow::Result<wreq::Response> {
        let req = self
            .client
            .get(url)
            .header("referer", origin_of(site).unwrap_or_default())
            .redirect(wreq::redirect::Policy::none())
            .timeout(Duration::from_secs(20));
        Ok(self.apply(req)?.send().await?)
    }

    /// GET a JSON endpoint using the same XHR headers as the site's player.
    pub async fn ajax_json(&self, url: &str, referer: &str) -> anyhow::Result<Value> {
        let req = self
            .client
            .get(url)
            .header("accept", ACCEPT_JSON)
            .header("x-requested-with", "XMLHttpRequest")
            .header("referer", referer)
            .timeout(Duration::from_secs(60));
        let resp = self.apply(req)?.send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        self.check_current()?;
        if !status.is_success() {
            anyhow::bail!("HTTP {status} for {url}");
        }
        serde_json::from_str(&body)
            .map_err(|error| anyhow::anyhow!("invalid JSON from {url}: {error}"))
    }

    /// POST a form and return the response body plus its HTTP status.
    pub async fn post_form(
        &self,
        url: &str,
        form: &[(&str, String)],
        referer: &str,
    ) -> anyhow::Result<(wreq::StatusCode, String)> {
        let req = self
            .client
            .post(url)
            .header("accept", ACCEPT_JSON)
            .header("content-type", "application/x-www-form-urlencoded")
            .header("x-requested-with", "XMLHttpRequest")
            .header("referer", referer)
            .timeout(Duration::from_secs(60));
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(form)
            .finish();
        let req = req.body(body);
        let resp = self.apply(req)?.send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        self.check_current()?;
        Ok((status, body))
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
        if !matches!(
            status,
            wreq::StatusCode::OK
                | wreq::StatusCode::PARTIAL_CONTENT
                | wreq::StatusCode::RANGE_NOT_SATISFIABLE
        ) {
            anyhow::bail!("media request for {url}: HTTP {status}");
        }
        let error_document = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                let content_type = v
                    .split(';')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase();
                content_type == "text/html"
                    || content_type == "application/json"
                    || content_type.ends_with("+json")
            });
        anyhow::ensure!(
            !error_document || status == wreq::StatusCode::RANGE_NOT_SATISFIABLE,
            "media request for {url}: error document instead of media"
        );
        Ok(resp)
    }
}

/// The cookie jar plus the login and warm-up state for one credential set.
#[derive(Clone)]
pub struct Session {
    current: Arc<Mutex<Arc<SessionGeneration>>>,
}

struct SessionGeneration {
    jar: Arc<wreq::cookie::Jar>,
    warmed: AtomicBool,
    logged_in: AtomicBool,
    /// Serialises the login exchange so concurrent workers share one attempt.
    login: tokio::sync::Mutex<()>,
    credentials: Option<(String, String)>,
    login_error: Mutex<Option<String>>,
    site_base: String,
}

impl Session {
    pub fn new() -> Self {
        Self {
            current: Arc::new(Mutex::new(Arc::new(SessionGeneration {
                jar: Arc::new(wreq::cookie::Jar::default()),
                warmed: AtomicBool::new(false),
                logged_in: AtomicBool::new(false),
                login: tokio::sync::Mutex::new(()),
                credentials: None,
                login_error: Mutex::new(None),
                site_base: String::new(),
            }))),
        }
    }

    /// Replace credentials atomically. Never clear/reuse a jar still owned by
    /// an in-flight request: its late Set-Cookie would resurrect the old session.
    pub fn set_configured(
        &self,
        cookie: Option<&str>,
        username: &str,
        password: &str,
        site_base: &str,
    ) {
        let jar = Arc::new(wreq::cookie::Jar::default());
        let site = site_base.trim();
        let mut has_consent = false;
        if let Some(cookie) = cookie {
            let cookie = cookie
                .trim()
                .strip_prefix("Cookie:")
                .or_else(|| cookie.trim().strip_prefix("cookie:"))
                .unwrap_or_else(|| cookie.trim())
                .trim();
            if !site.is_empty() {
                for pair in cookie.split([';', '\n', '\r']) {
                    let pair = pair.trim();
                    if !pair.is_empty() && pair.contains('=') {
                        if pair
                            .split_once('=')
                            .is_some_and(|(name, _)| name.trim() == "cookieConsent")
                        {
                            has_consent = true;
                        }
                        jar.add(format!("{pair}; Path=/"), site);
                    }
                }
            }
        }
        // Pornhub withholds the Recommended feed until its consent cookie is
        // present. This client never runs the site's advertising or analytics
        // scripts; the cookie only unlocks the server-rendered recommendations.
        if !site.is_empty() && !has_consent {
            jar.add("cookieConsent=3; Path=/", site);
        }
        let username = username.trim();
        // Keep the password byte-for-byte; only reject a wholly empty value.
        let credentials = (cookie.is_none() && !username.is_empty() && !password.trim().is_empty())
            .then(|| (username.to_string(), password.to_string()));
        *self.current.lock().expect("session lock poisoned") = Arc::new(SessionGeneration {
            jar,
            warmed: AtomicBool::new(false),
            logged_in: AtomicBool::new(false),
            login: tokio::sync::Mutex::new(()),
            credentials,
            login_error: Mutex::new(None),
            site_base: site.trim_end_matches('/').to_string(),
        });
    }

    /// True once the current cookie generation has established a site session.
    pub fn is_warm(&self) -> bool {
        self.current
            .lock()
            .expect("session lock poisoned")
            .warmed
            .load(Ordering::Acquire)
    }

    /// True after a successful form login or a page confirms a pasted cookie.
    pub fn is_logged_in(&self) -> bool {
        self.current
            .lock()
            .expect("session lock poisoned")
            .logged_in
            .load(Ordering::Acquire)
    }

    pub fn login_error(&self) -> Option<String> {
        self.current
            .lock()
            .expect("session lock poisoned")
            .login_error
            .lock()
            .expect("login error lock poisoned")
            .clone()
    }

    /// Only the generation that actually performed the login can become logged in.
    pub fn mark_warm(&self, fetch: &Fetcher) -> anyhow::Result<()> {
        let current = self.current.lock().expect("session lock poisoned");
        anyhow::ensure!(
            Arc::ptr_eq(&current, &fetch.generation),
            "Pornhub session changed; retry the operation"
        );
        current.warmed.store(true, Ordering::Release);
        Ok(())
    }

    /// Page observations belong to the credentials used to fetch that page.
    pub fn observe_login(&self, fetch: &Fetcher, logged_in: Option<bool>) -> anyhow::Result<()> {
        fetch.check_current()?;
        if let Some(logged_in) = logged_in {
            fetch
                .generation
                .logged_in
                .store(logged_in, Ordering::Release);
        }
        Ok(())
    }

    /// Wake a cold session and perform the account login when one is configured.
    /// Concurrent callers within one generation share a single attempt.
    pub async fn ensure(&self, url: &str, referer: &str, fetch: &Fetcher) -> anyhow::Result<()> {
        fetch.check_current()?;
        let generation = &fetch.generation;
        let _guard = generation.login.lock().await;
        fetch.check_current()?;
        if generation.warmed.load(Ordering::Acquire) {
            return Ok(());
        }
        if generation.credentials.is_some()
            && let Err(error) = self.login_locked(generation, fetch).await
        {
            Self::record_login_error(generation, &error);
            return Err(error);
        }
        fetch.text(url, Some(referer)).await?;
        self.mark_warm(fetch)
    }

    /// Wake a cold session and return the requested page's HTML. The listing
    /// path uses this so warming and parsing share one request.
    pub async fn ensure_text(
        &self,
        url: &str,
        referer: &str,
        fetch: &Fetcher,
    ) -> anyhow::Result<String> {
        fetch.check_current()?;
        let generation = &fetch.generation;
        let _guard = generation.login.lock().await;
        fetch.check_current()?;
        if !generation.warmed.load(Ordering::Acquire)
            && generation.credentials.is_some()
            && let Err(error) = self.login_locked(generation, fetch).await
        {
            Self::record_login_error(generation, &error);
            return Err(error);
        }
        let html = fetch.text(url, Some(referer)).await?;
        self.mark_warm(fetch)?;
        Ok(html)
    }

    /// Re-run the login exchange after a page reports a signed-out session.
    pub async fn relogin(&self, fetch: &Fetcher) -> anyhow::Result<()> {
        fetch.check_current()?;
        let generation = &fetch.generation;
        let _guard = generation.login.lock().await;
        fetch.check_current()?;
        if generation.credentials.is_none() {
            return Ok(());
        }
        generation.logged_in.store(false, Ordering::Release);
        generation.warmed.store(false, Ordering::Release);
        if let Err(error) = self.login_locked(generation, fetch).await {
            Self::record_login_error(generation, &error);
            return Err(error);
        }
        Ok(())
    }

    async fn login_locked(
        &self,
        generation: &SessionGeneration,
        fetch: &Fetcher,
    ) -> anyhow::Result<()> {
        if generation.logged_in.load(Ordering::Acquire) {
            return Ok(());
        }
        let (Some((username, password)), false) =
            (&generation.credentials, generation.site_base.is_empty())
        else {
            return Ok(());
        };
        let site = &generation.site_base;
        let login_page = format!("{site}/login");
        let html = fetch
            .text(&login_page, Some(&format!("{site}/")))
            .await
            .map_err(|error| anyhow::anyhow!("cannot load the Pornhub login page: {error:#}"))?;
        let form = scraper::parse_login_form(&html).ok_or_else(|| {
            anyhow::anyhow!("the Pornhub login form was not found — the markup may have changed")
        })?;
        let authenticate = format!("{site}/front/authenticate");
        let fields = [
            ("email", username.clone()),
            ("password", password.clone()),
            ("token", form.token.clone()),
            ("redirect", form.redirect.clone()),
            ("from", form.from.clone()),
            ("intended_action", form.intended_action.clone()),
        ];
        let (status, body) = fetch
            .post_form(&authenticate, &fields, &login_page)
            .await
            .map_err(|error| anyhow::anyhow!("Pornhub login request failed: {error:#}"))?;
        let result = serde_json::from_str::<Value>(&body).map_err(|_| {
            anyhow::anyhow!(
                "Pornhub login returned HTTP {status} instead of JSON; the account may need a CAPTCHA"
            )
        })?;
        if result
            .get("requireCaptcha")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let message = result
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("the site requires a CAPTCHA");
            return Err(anyhow::anyhow!(
                "Pornhub login requires a CAPTCHA ({message}); configure a session cookie instead"
            ));
        }
        if result
            .get("twoStepVerification")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err(anyhow::anyhow!(
                "Pornhub login requires two-step verification; configure a session cookie instead"
            ));
        }
        let redirect = result
            .get("redirect")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string();
        if redirect.is_empty() {
            let message = result
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("the site rejected the account credentials");
            return Err(anyhow::anyhow!("Pornhub login failed: {message}"));
        }
        // Visiting the redirect completes the same cookie handshake a browser
        // performs; the session cookies themselves normally arrive above.
        let redirect = url::Url::parse(&redirect)
            .or_else(|_| url::Url::parse(&format!("{site}/")).and_then(|base| base.join(&redirect)))
            .map_err(|error| anyhow::anyhow!("invalid Pornhub login redirect: {error}"))?;
        fetch.text(redirect.as_str(), Some(&login_page)).await?;
        generation.logged_in.store(true, Ordering::Release);
        *generation
            .login_error
            .lock()
            .expect("login error lock poisoned") = None;
        log::info!("Pornhub account login succeeded");
        Ok(())
    }

    fn record_login_error(generation: &SessionGeneration, error: &anyhow::Error) {
        let message = format!("{error:#}");
        log::warn!("Pornhub login failed: {message}");
        *generation
            .login_error
            .lock()
            .expect("login error lock poisoned") = Some(message);
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}
