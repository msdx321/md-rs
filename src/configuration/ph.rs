//! Pornhub settings. The module downloads the site's progressive MP4 variants,
//! and defaults to the personalised Recommended feed. An account is optional:
//! guests can still download the public feed, but recommendations are richer
//! when the configured account is signed in.
use std::path::PathBuf;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use super::listing::{page_suffix, title_matcher};

fn followed_path(url: &url::Url) -> Option<String> {
    let parts: Vec<_> = url.path().trim_matches('/').split('/').collect();
    match parts.as_slice() {
        [kind @ ("channels" | "pornstar" | "model"), name]
        | [kind @ ("channels" | "pornstar" | "model"), name, "videos"]
            if !name.is_empty() =>
        {
            Some(format!("/{kind}/{name}/videos"))
        }
        _ => None,
    }
}

pub const FILE: crate::configuration::ConfigFile<Config> =
    crate::configuration::ConfigFile::new(crate::configuration::PH_FILE).with_groups(&[
        &["cookie", "password", "site_base", "user_agent", "username"],
        &["links", "max_pages", "popular_path", "top_n"],
        &[
            "min_duration_secs",
            "quality",
            "title_filter",
            "title_filter_regex",
        ],
        &["concurrent_videos"],
    ]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListingLink {
    pub url: String,
    pub daily_quota: usize,
}

/// Runtime configuration, loaded from `config/ph.yaml`.
///
/// Every field has a default so a missing or partial file still boots; the
/// YAML saves omit default values; the web API returns the complete model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    // ── site access ───────────────────────────────────────────────────────
    /// Base URL of the site, without a trailing slash.
    pub site_base: String,
    /// Listing path used by the scheduled job and the listing tab. Defaults to
    /// the personalised Recommended feed.
    pub popular_path: String,
    /// Listing links with independent quotas. Empty uses popular_path/top_n.
    pub links: Vec<ListingLink>,
    /// Optional full Cookie header. When set, it is used instead of the account
    /// login (useful for accounts protected by a CAPTCHA or two-factor step).
    pub cookie: String,
    /// Account email used by the site's login form.
    pub username: String,
    pub password: String,
    /// Optional user agent override.
    pub user_agent: String,

    // ── scheduled job ─────────────────────────────────────────────────────
    /// Target number of new completed downloads per run, in listing order.
    pub top_n: usize,
    /// Include only titles matching this text or regex; empty disables filtering.
    pub title_filter: String,
    pub title_filter_regex: bool,
    /// Exclude listing cards with a known duration below this many seconds.
    pub min_duration_secs: f64,
    /// How many listing pages to scan when looking for candidates.
    pub max_pages: usize,
    /// `highest` or a height cap such as `1080`, `720`, `480`, `360`.
    pub quality: String,

    // ── download ──────────────────────────────────────────────────────────
    #[serde(skip)]
    pub save_path: PathBuf,
    /// Partial files live next to the finished output so disk space is shared.
    #[serde(skip)]
    pub temp_path: PathBuf,
    /// How many videos may download at the same time.
    pub concurrent_videos: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            site_base: "https://www.pornhub.com".into(),
            popular_path: "/recommended".into(),
            links: Vec::new(),
            cookie: String::new(),
            username: String::new(),
            password: String::new(),
            user_agent: String::new(),
            top_n: 10,
            title_filter: String::new(),
            title_filter_regex: false,
            min_duration_secs: 0.0,
            max_pages: 2,
            quality: "highest".into(),
            save_path: PathBuf::from("downloads/ph"),
            temp_path: PathBuf::from("temp"),
            concurrent_videos: 1,
        }
    }
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!((1..=100).contains(&self.top_n), "top_n must be 1..=100");
        anyhow::ensure!(
            (1..=20).contains(&self.max_pages),
            "max_pages must be 1..=20"
        );
        anyhow::ensure!(
            self.min_duration_secs.is_finite() && self.min_duration_secs >= 0.0,
            "min_duration_secs must be nonnegative"
        );
        anyhow::ensure!(
            (1..=8).contains(&self.concurrent_videos),
            "concurrent_videos must be 1..=8"
        );
        let quality = self.quality.trim().to_ascii_lowercase();
        anyhow::ensure!(
            matches!(quality.as_str(), "highest" | "best")
                || self
                    .target_height()
                    .is_some_and(|height| (144..=16384).contains(&height)),
            "quality must be highest or a height between 144 and 16384"
        );
        anyhow::ensure!(
            self.username.trim().is_empty() == self.password.trim().is_empty(),
            "account email and password must be set together"
        );
        self.title_matcher()?;
        self.validate_links()
    }

    pub fn listing_links(&self) -> Vec<ListingLink> {
        if self.links.is_empty() {
            vec![ListingLink {
                url: self.popular_path.clone(),
                daily_quota: self.top_n,
            }]
        } else {
            self.links.clone()
        }
    }

    pub fn for_link(&self, link: &ListingLink) -> Self {
        Self {
            popular_path: link.url.clone(),
            top_n: link.daily_quota,
            links: Vec::new(),
            ..self.clone()
        }
    }

    fn validate_links(&self) -> anyhow::Result<()> {
        let base = url::Url::parse(&self.site_base).context("invalid site base URL")?;
        let mut followed = std::collections::HashSet::new();
        anyhow::ensure!(
            matches!(base.scheme(), "http" | "https") && base.host_str().is_some(),
            "site_base must be an HTTP(S) URL"
        );
        for link in self.listing_links() {
            anyhow::ensure!(
                (1..=100).contains(&link.daily_quota),
                "daily quota must be 1..=100"
            );
            anyhow::ensure!(
                !link.url.trim().is_empty(),
                "listing link must not be empty"
            );
            let cfg = self.for_link(&link);
            let url = url::Url::parse(&cfg.popular_url(1)).context("invalid listing link")?;
            anyhow::ensure!(
                matches!(url.scheme(), "http" | "https") && url.origin() == base.origin(),
                "listing links must use the same origin as site_base"
            );
            if let Some(source) = cfg.followed_source() {
                anyhow::ensure!(
                    followed.insert(source),
                    "channel or model is listed more than once"
                );
            }
        }
        Ok(())
    }

    /// Compile once per listing request, using literal case-insensitive text by default.
    pub fn title_matcher(&self) -> anyhow::Result<Option<regex::Regex>> {
        title_matcher(&self.title_filter, self.title_filter_regex)
    }

    /// Absolute listing URL for `page` (1-based).
    pub fn popular_url(&self, page: usize) -> String {
        let base = self.site_base.trim_end_matches('/');
        let path = self.popular_path.trim();
        let path = if path.is_empty() { "recommended" } else { path };
        let raw = if path.starts_with("https://") || path.starts_with("http://") {
            path.to_string()
        } else {
            format!("{base}/{}", path.trim_start_matches('/'))
        };
        if let Ok(mut url) = url::Url::parse(&raw)
            && let Some(path) = followed_path(&url)
        {
            // Profile video pages default to Most Recent. A pasted ranking or
            // page parameter must not turn an upload subscription into a top list.
            url.set_path(&path);
            url.set_query(None);
            return page_suffix(url.as_str(), page);
        }
        page_suffix(&raw, page)
    }

    pub fn followed_source(&self) -> Option<String> {
        let url = url::Url::parse(&self.popular_url(1)).ok()?;
        followed_path(&url)?;
        Some(url.into())
    }

    /// Page URL for a video key (`ph...`).
    pub fn video_url(&self, id: &str) -> String {
        format!(
            "{}/view_video.php?viewkey={id}",
            self.site_base.trim_end_matches('/')
        )
    }

    /// Imported cookie header, preserving the account's cookie values.
    pub fn request_cookie(&self) -> Option<String> {
        let configured = self.cookie.trim().trim_end_matches(';').trim();
        (!configured.is_empty()).then(|| configured.to_string())
    }

    /// The account is usable only when both fields are present.
    pub fn account(&self) -> Option<(&str, &str)> {
        let username = self.username.trim();
        // Passwords may legitimately contain leading or trailing spaces; only
        // reject a wholly empty value.
        (!username.is_empty() && !self.password.trim().is_empty())
            .then_some((username, self.password.as_str()))
    }

    /// Cookie override or account credentials: either can establish access.
    pub fn access_configured(&self) -> bool {
        self.request_cookie().is_some() || self.account().is_some()
    }

    /// Requested height cap, or `None` for the highest available variant.
    pub fn target_height(&self) -> Option<u64> {
        let quality = self.quality.trim().to_ascii_lowercase();
        if quality.is_empty() || quality == "highest" || quality == "best" {
            return None;
        }
        quality
            .trim_end_matches('p')
            .parse()
            .ok()
            .filter(|&h| h > 0)
    }
}
