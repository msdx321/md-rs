use std::path::PathBuf;

use anyhow::Context;
use serde::{Deserialize, Serialize};

pub const FILE: crate::ConfigFile<Config> = crate::ConfigFile::new(crate::JAV_FILE);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListingLink {
    pub url: String,
    pub daily_quota: usize,
}

/// Runtime configuration, loaded from `config/jav.yaml`.
///
/// Every field has a default so a missing or partial file still boots; the
/// YAML saves omit default values; the web API returns the complete model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    // ── site access ───────────────────────────────────────────────────────
    /// Base URL of the site, without a trailing slash.
    pub site_base: String,
    /// Path of the ranking listing, including its sort query, e.g.
    /// `/dm597/cn/fc2?sort=weekly_views`. The daily job takes the first `top_n`
    /// entries in the order the site returns them.
    pub popular_path: String,
    /// Ranking links with independent quotas. Empty uses popular_path/top_n.
    pub links: Vec<ListingLink>,
    /// Optional `cf_clearance` override (a bare value, `cf_clearance=…`, or a
    /// full `Cookie:` header).
    ///
    /// Normally left empty: the service drives headless Chromium to clear
    /// Cloudflare and keeps the cookie it receives. A value here just seeds the
    /// first request, or stands in when automatic minting is disabled.
    pub cookie: String,
    /// Optional user agent override.
    ///
    /// Empty means "use the browser's own", which is the coherent choice. Set it
    /// only to deliberately impersonate a different browser: it must then agree
    /// with the TLS fingerprint, and it will be sent to the minting browser too.
    pub user_agent: String,
    /// Chromium/Chrome binary for cookie minting. Empty means auto-detect.
    pub browser_path: PathBuf,
    /// Profile directory deleted and recreated before every cookie mint.
    pub browser_profile_dir: PathBuf,
    /// Turn off browser-driven cookie minting entirely.
    pub browser_enabled: bool,

    // ── daily job ─────────────────────────────────────────────────────────
    /// Target number of new completed downloads per run, in ranking order.
    pub top_n: usize,
    /// Include only titles matching this text or regex; empty disables filtering.
    pub title_filter: String,
    pub title_filter_regex: bool,
    /// Streams shorter than this are treated as previews and rejected.
    pub min_duration_secs: f64,
    /// How many listing pages to scan when looking for candidates.
    pub max_pages: usize,
    // ── download ──────────────────────────────────────────────────────────
    #[serde(skip)]
    pub save_path: PathBuf,
    /// HLS work directory, separate from completed downloads.
    #[serde(skip)]
    pub temp_path: PathBuf,
    /// How many videos may download at the same time.
    pub concurrent_videos: usize,
    /// How many HLS segments of one video may be fetched at the same time.
    pub segment_concurrency: usize,
    /// `highest`, `lowest`, or a target height such as `1080`.
    pub resolution: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            site_base: "https://missav.ai".into(),
            // The ranking lives in the sort query: MissAV has no view count in
            // the markup, so the server's ordering *is* the ranking.
            popular_path: "/dm597/cn/fc2?sort=weekly_views".into(),
            links: Vec::new(),
            cookie: String::new(),
            user_agent: String::new(),
            browser_path: PathBuf::new(),
            browser_profile_dir: PathBuf::from("browser-profile"),
            browser_enabled: true,
            top_n: 10,
            title_filter: String::new(),
            title_filter_regex: false,
            min_duration_secs: 600.0,
            max_pages: 2,
            save_path: PathBuf::from("downloads/jav"),
            temp_path: PathBuf::from("temp"),
            concurrent_videos: 1,
            segment_concurrency: 8,
            resolution: "highest".into(),
        }
    }
}

impl Config {
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

    pub fn validate_links(&self) -> anyhow::Result<()> {
        for link in &self.links {
            anyhow::ensure!(
                (1..=100).contains(&link.daily_quota),
                "daily quota must be 1..=100"
            );
            anyhow::ensure!(
                !link.url.trim().is_empty(),
                "ranking link must not be empty"
            );
            let cfg = self.for_link(link);
            let url = url::Url::parse(&cfg.popular_url(1)).context("invalid ranking link")?;
            let base = url::Url::parse(&self.site_base).context("invalid site base URL")?;
            anyhow::ensure!(
                matches!(url.scheme(), "http" | "https") && url.origin() == base.origin(),
                "ranking links must use the same origin as site_base"
            );
        }
        Ok(())
    }

    /// Compile once per listing request, using literal case-insensitive text by default.
    pub fn title_matcher(&self) -> anyhow::Result<Option<regex::Regex>> {
        if self.title_filter.trim().is_empty() {
            return Ok(None);
        }
        let pattern = if self.title_filter_regex {
            self.title_filter.clone()
        } else {
            regex::escape(&self.title_filter)
        };
        regex::RegexBuilder::new(&pattern)
            .case_insensitive(!self.title_filter_regex)
            .build()
            .map(Some)
            .context("invalid title filter regex")
    }

    /// Absolute listing URL for `page` (1-based) of the popular ranking.
    pub fn popular_url(&self, page: usize) -> String {
        let base = self.site_base.trim_end_matches('/');
        let path = self.popular_path.trim();
        if path.starts_with("https://") || path.starts_with("http://") {
            return page_suffix(path, page);
        }
        let path = if path.is_empty() { "popular" } else { path };
        page_suffix(&format!("{base}/{}", path.trim_start_matches('/')), page)
    }

    /// Download page URL for a video slug (`fc2-ppv-4968310`), placed under the
    /// same language subtree as the listing so links stay consistent.
    pub fn video_url(&self, id: &str) -> String {
        let base = self.site_base.trim_end_matches('/');
        match self.language_filter() {
            Some(lang) => format!("{base}/{lang}/{id}"),
            None => format!("{base}/{id}"),
        }
    }

    /// The language subtree of `popular_path` (`/dm597/cn/fc2` → `cn`).
    ///
    /// Used to keep parsing on one language, so the language-switcher links that
    /// appear on every listing page cannot inject duplicate cards.
    pub fn language_filter(&self) -> Option<String> {
        const LANGS: [&str; 14] = [
            "cn", "en", "ja", "ko", "ms", "th", "de", "fr", "vi", "id", "fil", "pt", "zh", "es",
        ];
        self.popular_path
            .split('?')
            .next()
            .unwrap_or_default()
            .split('/')
            .find(|seg| LANGS.contains(&seg.to_ascii_lowercase().as_str()))
            .map(|seg| seg.to_ascii_lowercase())
    }
}

/// Append the page number as a query parameter.
///
/// MissAV pages listings with `?page=N`.
fn page_suffix(path: &str, page: usize) -> String {
    // Replace an existing page parameter when a pasted link is already paged.
    if let Ok(mut url) = url::Url::parse(path) {
        let pairs: Vec<_> = url
            .query_pairs()
            .filter(|(key, _)| key != "page")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        url.set_query(None);
        if !pairs.is_empty() || page > 1 {
            let mut query = url.query_pairs_mut();
            query.extend_pairs(pairs);
            if page > 1 {
                query.append_pair("page", &page.to_string());
            }
        }
        url.set_fragment(None);
        return url.into();
    }
    if page <= 1 {
        return path.to_string();
    }
    if path.contains('?') {
        format!("{path}&page={page}")
    } else {
        format!("{path}?page={page}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn popular_url_builds_pagination() {
        let cfg = Config::default();
        assert_eq!(
            cfg.popular_url(1),
            "https://missav.ai/dm597/cn/fc2?sort=weekly_views"
        );
        assert_eq!(
            cfg.popular_url(3),
            "https://missav.ai/dm597/cn/fc2?sort=weekly_views&page=3",
            "paged listings keep the sort query"
        );
    }

    #[test]
    fn popular_url_without_query_still_pages() {
        let cfg = Config {
            popular_path: "/dm597/cn/fc2".into(),
            ..Config::default()
        };
        assert_eq!(cfg.popular_url(1), "https://missav.ai/dm597/cn/fc2");
        assert_eq!(cfg.popular_url(2), "https://missav.ai/dm597/cn/fc2?page=2");
    }

    #[test]
    fn video_url_uses_the_listing_language() {
        let cfg = Config::default();
        assert_eq!(
            cfg.video_url("fc2-ppv-4968310"),
            "https://missav.ai/cn/fc2-ppv-4968310"
        );
    }

    #[test]
    fn video_url_omits_language_when_the_listing_has_none() {
        let cfg = Config {
            popular_path: "/dm597/fc2".into(),
            ..Config::default()
        };
        assert_eq!(
            cfg.video_url("fc2-ppv-4968310"),
            "https://missav.ai/fc2-ppv-4968310"
        );
    }

    #[test]
    fn language_filter_reads_the_subtree() {
        let cfg = Config::default();
        assert_eq!(cfg.language_filter().as_deref(), Some("cn"));

        let en = Config {
            popular_path: "/dm597/en/fc2?sort=weekly_views".into(),
            ..Config::default()
        };
        assert_eq!(en.language_filter().as_deref(), Some("en"));

        let none = Config {
            popular_path: "/dm597/fc2".into(),
            ..Config::default()
        };
        assert_eq!(none.language_filter(), None);
    }
}
