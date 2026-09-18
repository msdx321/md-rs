//! 91Porn settings. The module downloads progressive MP4 files, so unlike the
//! JAV provider there is no stream-quality selection: the only choice is
//! whether to prefer the site's separate HD page.
use std::path::PathBuf;

use anyhow::Context;
use serde::{Deserialize, Serialize};

pub const FILE: crate::configuration::ConfigFile<Config> =
    crate::configuration::ConfigFile::new(crate::configuration::P91_FILE).with_groups(&[
        &["cookie", "site_base", "user_agent"],
        &["links", "max_pages", "popular_path", "top_n"],
        &[
            "hd_only",
            "min_duration_secs",
            "prefer_hd",
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

/// Runtime configuration, loaded from `config/p91.yaml`.
///
/// Every field has a default so a missing or partial file still boots; the
/// YAML saves omit default values; the web API returns the complete model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    // ── site access ───────────────────────────────────────────────────────
    /// Base URL of the site, without a trailing slash.
    pub site_base: String,
    /// Category listing path, including its query, e.g.
    /// `/v.php?category=top&viewtype=basic`.
    pub popular_path: String,
    /// Listing links with independent quotas. Empty uses popular_path/top_n.
    pub links: Vec<ListingLink>,
    /// Full browser Cookie header. HD requires a VIP account; empty browses as a guest.
    pub cookie: String,
    /// Optional user agent override.
    pub user_agent: String,

    // ── scheduled job ─────────────────────────────────────────────────────
    /// Target number of new completed downloads per run, in listing order.
    pub top_n: usize,
    /// Include only titles matching this text or regex; empty disables filtering.
    pub title_filter: String,
    pub title_filter_regex: bool,
    /// Keep only cards the site flags as HD. Listings without enough HD entries
    /// simply yield fewer candidates per run.
    pub hd_only: bool,
    /// Exclude listing cards with a known duration below this many seconds.
    pub min_duration_secs: f64,
    /// How many listing pages to scan when looking for candidates.
    pub max_pages: usize,
    /// Fetch the HD page first. Falls back to the standard page when it yields
    /// no source (guest, or a video without an HD version).
    pub prefer_hd: bool,

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
            site_base: "https://www.91porn.com".into(),
            popular_path: "/v.php?category=top&viewtype=basic".into(),
            links: Vec::new(),
            cookie: String::new(),
            user_agent: String::new(),
            top_n: 10,
            title_filter: String::new(),
            title_filter_regex: false,
            hd_only: false,
            min_duration_secs: 0.0,
            max_pages: 2,
            prefer_hd: true,
            save_path: PathBuf::from("downloads/p91"),
            temp_path: PathBuf::from("temp"),
            concurrent_videos: 1,
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
        let base = url::Url::parse(&self.site_base).context("invalid site base URL")?;
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

    /// Absolute listing URL for `page` (1-based).
    pub fn popular_url(&self, page: usize) -> String {
        let base = self.site_base.trim_end_matches('/');
        let path = self.popular_path.trim();
        if path.starts_with("https://") || path.starts_with("http://") {
            return page_suffix(path, page);
        }
        let path = if path.is_empty() {
            "v.php?category=top&viewtype=basic"
        } else {
            path
        };
        page_suffix(&format!("{base}/{}", path.trim_start_matches('/')), page)
    }

    /// Page URL for a video key (`1950234389`).
    pub fn video_url(&self, id: &str) -> String {
        format!(
            "{}/view_video.php?viewkey={id}",
            self.site_base.trim_end_matches('/')
        )
    }

    /// HD variant of the video page. The server withholds its source from
    /// guests, so this requires cookies from an account with VIP access.
    pub fn hd_url(&self, id: &str) -> String {
        format!(
            "{}/view_video_hd.php?viewkey={id}",
            self.site_base.trim_end_matches('/')
        )
    }

    /// Imported cookie header, preserving the account's cookie values.
    pub fn request_cookie(&self) -> Option<String> {
        let configured = self.cookie.trim().trim_end_matches(';').trim();
        (!configured.is_empty()).then(|| configured.to_string())
    }
}

/// Append the page number as a query parameter, replacing a pasted `page`.
fn page_suffix(path: &str, page: usize) -> String {
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
            "https://www.91porn.com/v.php?category=top&viewtype=basic"
        );
        assert_eq!(
            cfg.popular_url(2),
            "https://www.91porn.com/v.php?category=top&viewtype=basic&page=2"
        );
    }

    #[test]
    fn video_urls_use_the_configured_site() {
        let cfg = Config::default();
        assert_eq!(
            cfg.video_url("1950234389"),
            "https://www.91porn.com/view_video.php?viewkey=1950234389"
        );
        assert_eq!(
            cfg.hd_url("1950234389"),
            "https://www.91porn.com/view_video_hd.php?viewkey=1950234389"
        );
    }

    #[test]
    fn request_cookie_preserves_imported_values() {
        let cfg = Config {
            cookie: "PHPSESSID=abc".into(),
            ..Config::default()
        };
        assert_eq!(cfg.request_cookie().as_deref(), Some("PHPSESSID=abc"));
        let real_level = Config {
            cookie: "CLIPSHARE=abc; level=opaque".into(),
            ..Config::default()
        };
        assert_eq!(
            real_level.request_cookie().as_deref(),
            Some("CLIPSHARE=abc; level=opaque"),
            "an account's own level must not be overwritten"
        );
        let guest = Config {
            cookie: String::new(),
            ..Config::default()
        };
        assert_eq!(guest.request_cookie(), None);
    }
}
