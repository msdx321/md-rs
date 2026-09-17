//! Scraping the MissAV listing.
//!
//! Listing pages are server-rendered grids of `div.thumbnail` cards. Each card
//! repeats the same detail link several times (thumbnail, duration overlay,
//! headline) and carries the cover image plus a duration badge.
//!
//! Ranking is **not** derived from the page: MissAV has no view count in the
//! markup. Instead the listing itself is requested pre-sorted by the site
//! (`?sort=weekly_views`, `?sort=monthly_views`, …), so the server's order *is*
//! the ranking and the daily job simply takes the first N.
//!
//! Card shape (abridged):
//!
//! ```html
//! <div class="thumbnail group">
//!   <a href="https://missav.ai/cn/fc2-ppv-4968310" alt="fc2-ppv-4968310">
//!     <img class="w-full" data-src="https://fourhoi.com/fc2-ppv-4968310/cover-t.jpg" src="…" alt="…">
//!   </a>
//!   <a href="…/cn/fc2-ppv-4968310"><span …>1:16:54</span></a>
//!   <div class="my-2 …"><a href="…/cn/fc2-ppv-4968310">FC2-PPV-4968310 …</a></div>
//! </div>
//! ```

use anyhow::Context;
use dom_query::Document;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::config::Config;
use crate::source::http::Fetcher;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoCard {
    /// Detail slug taken from the URL (`…/cn/fc2-ppv-4968310`).
    pub id: String,
    pub url: String,
    pub title: String,
    pub image_url: String,
    /// Runtime badge ("1:16:54") in seconds; `None` when the card omits it.
    pub duration_secs: Option<u64>,
    /// 1-based position in the listing. The listing is requested pre-sorted by
    /// the site, so this *is* the popularity rank; MissAV exposes no view count.
    pub rank: Option<usize>,
}

/// Turn a `H:MM:SS` / `M:SS` duration badge into seconds.
pub fn parse_duration(text: &str) -> Option<u64> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let mut total = 0u64;
    let mut parts = 0;
    for part in text.split(':') {
        let value: u64 = part.trim().parse().ok()?;
        total = total.checked_mul(60)?.checked_add(value)?;
        parts += 1;
    }
    (parts >= 2).then_some(total)
}

/// Extract the detail slug from a MissAV video URL.
///
/// Video pages are `/<lang>/<slug>` (or `/<slug>`), where the slug is
/// separated by hyphens or underscores: `fc2-ppv-4968310`, `091326_001`.
/// Section pages such as `/cn/actresses` have neither and are rejected, keeping navigation
/// links out of the results.
pub fn video_id_from_url(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let host = parsed.host_str()?.to_ascii_lowercase();
    if !matches!(parsed.scheme(), "http" | "https")
        || !(host == "missav.ai" || host.ends_with(".missav.ai"))
    {
        return None;
    }
    let slug = parsed.path_segments()?.rfind(|s| !s.is_empty())?;
    is_video_slug(slug).then(|| slug.to_string())
}

/// A slug looks like a catalogue number: lowercase alphanumerics in at least
/// two groups separated by hyphens or underscores.
fn is_video_slug(slug: &str) -> bool {
    if slug.len() < 3 || slug.contains('.') {
        return false;
    }
    let mut groups = 0;
    for group in slug.split(['-', '_']) {
        if group.is_empty() || !group.chars().all(|c| c.is_ascii_alphanumeric()) {
            return false;
        }
        groups += 1;
    }
    groups >= 2
}

/// The language segment of a video URL, when present (`…/cn/…` → `cn`).
fn language_of(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let segments: Vec<&str> = parsed.path_segments()?.filter(|s| !s.is_empty()).collect();
    // /<lang>/<slug> → the language; /<slug> → none.
    (segments.len() >= 2).then(|| segments[segments.len() - 2].to_string())
}

/// Pure parser for a listing page, without a language restriction.
#[cfg(test)]
pub fn parse_list(html: &str) -> Vec<VideoCard> {
    parse_list_for_language(html, None)
}

/// Parse a listing, optionally keeping only one language subtree so that the
/// language switcher links on the page cannot smuggle in duplicates.
pub fn parse_list_for_language(html: &str, language: Option<&str>) -> Vec<VideoCard> {
    let doc = Document::from(html);
    let mut cards: Vec<VideoCard> = Vec::new();

    for card in doc.select("div.thumbnail").iter() {
        // Every detail link for this card, including the headline one.
        let anchors = card.select("a[href*=\"missav.ai\"]");
        let mut url = None;
        let mut id = None;
        for anchor in anchors.iter() {
            let Some(href) = anchor.attr("href").map(|v| v.to_string()) else {
                continue;
            };
            let Some(slug) = video_id_from_url(&href) else {
                continue;
            };
            if let Some(want) = language
                && language_of(&href).as_deref() != Some(want)
            {
                continue;
            }
            url = Some(href);
            id = Some(slug);
            break;
        }
        let (Some(url), Some(id)) = (url, id) else {
            continue;
        };
        if cards.iter().any(|c| c.id == id) {
            continue;
        }

        // The headline anchor carries the full title; the cover's alt is the
        // fallback and is sometimes truncated.
        let mut title = String::new();
        for anchor in card.select("div a").iter() {
            let text = anchor.text().to_string().trim().to_string();
            if !text.is_empty() && !looks_like_duration(&text) {
                title = text;
                break;
            }
        }
        if title.is_empty() {
            title = card
                .select("img")
                .iter()
                .next()
                .and_then(|img| img.attr("alt").map(|v| v.to_string()))
                .unwrap_or_default()
                .trim()
                .to_string();
        }
        if title.is_empty() {
            title = id.clone();
        }

        let image_url = card
            .select("img")
            .iter()
            .next()
            .and_then(|img| {
                img.attr("data-src")
                    .or_else(|| img.attr("src"))
                    .map(|v| v.to_string())
            })
            .unwrap_or_default();

        let duration_secs = card
            .select("span")
            .iter()
            .filter_map(|span| parse_duration(span.text().as_ref()))
            .next();

        cards.push(VideoCard {
            id,
            url,
            title,
            image_url,
            duration_secs,
            rank: Some(cards.len() + 1),
        });
    }

    cards
}

fn looks_like_duration(text: &str) -> bool {
    parse_duration(text).is_some()
}

/// Fetch one page of the listing.
pub async fn fetch_popular(
    fetch: &Fetcher,
    cfg: &Config,
    page: usize,
) -> anyhow::Result<Vec<VideoCard>> {
    let matcher = cfg.title_matcher()?;
    let url = cfg.popular_url(page);
    let html = fetch
        .text(
            &url,
            Some(&format!("{}/", cfg.site_base.trim_end_matches('/'))),
        )
        .await
        .with_context(|| format!("failed to fetch {url}"))?;
    let mut cards = parse_list_for_language(&html, cfg.language_filter().as_deref());
    if cards.is_empty() {
        anyhow::bail!("no videos parsed from {url} — the markup may have changed");
    }
    if let Some(matcher) = matcher {
        cards.retain(|card| matcher.is_match(&card.title));
    }
    Ok(cards)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The real listing captured from `…/dm597/cn/fc2?sort=weekly_views`.
    const LIVE_LISTING: &str = include_str!("../../tests/fixtures/missav_listing.html");

    const LIST: &str = r#"
    <div class="grid">
      <div>
        <div class="thumbnail group">
          <a href="https://missav.ai/cn/fc2-ppv-4968310" alt="fc2-ppv-4968310">
            <img class="w-full" data-src="https://fourhoi.com/fc2-ppv-4968310/cover-t.jpg" alt="FC2-PPV-4968310 full title">
          </a>
          <a href="https://missav.ai/cn/fc2-ppv-4968310"><span class="badge">1:16:54</span></a>
          <div class="my-2 truncate"><a href="https://missav.ai/cn/fc2-ppv-4968310">FC2-PPV-4968310 full title</a></div>
        </div>
      </div>
      <div>
        <div class="thumbnail group">
          <a href="https://missav.ai/cn/abp-123" alt="abp-123">
            <img data-src="https://fourhoi.com/abp-123/cover-t.jpg" alt="ABP-123 other">
          </a>
          <a href="https://missav.ai/cn/abp-123"><span class="badge">45:00</span></a>
          <div class="my-2 truncate"><a href="https://missav.ai/cn/abp-123">ABP-123 other</a></div>
        </div>
      </div>
      <div><a href="https://missav.ai/cn/actresses">Actresses</a></div>
    </div>"#;

    #[test]
    fn parses_cards_from_the_live_listing() {
        let cards = parse_list_for_language(LIVE_LISTING, Some("cn"));
        assert!(
            cards.len() >= 10,
            "expected a full page of cards, got {}",
            cards.len()
        );
        let first = &cards[0];
        assert_eq!(first.id, "fc2-ppv-4968310");
        assert_eq!(first.url, "https://missav.ai/cn/fc2-ppv-4968310");
        assert!(
            first.title.starts_with("FC2-PPV-4968310"),
            "title should keep the catalogue number: {:?}",
            first.title
        );
        assert!(
            first.image_url.starts_with("https://fourhoi.com/"),
            "unexpected cover: {:?}",
            first.image_url
        );
        // Every card is a distinct video.
        let mut ids: Vec<&str> = cards.iter().map(|c| c.id.as_str()).collect();
        let before = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), before, "duplicate cards leaked through");
    }

    #[test]
    fn live_listing_titles_are_not_bare_slugs() {
        let cards = parse_list_for_language(LIVE_LISTING, Some("cn"));
        // A title that fell back to the id would mean the headline selector
        // stopped matching, which is the failure mode worth catching.
        let fallbacks = cards.iter().filter(|c| c.title == c.id).count();
        assert_eq!(fallbacks, 0, "headline titles were not parsed");
    }

    #[test]
    fn parses_synthetic_cards() {
        let cards = parse_list(LIST);
        assert_eq!(cards.len(), 2, "section links must not become cards");
        assert_eq!(cards[0].id, "fc2-ppv-4968310");
        assert_eq!(cards[0].title, "FC2-PPV-4968310 full title");
        assert_eq!(cards[0].duration_secs, Some(3600 + 16 * 60 + 54));
        assert_eq!(cards[0].rank, Some(1));
        assert_eq!(cards[1].rank, Some(2));
        assert_eq!(cards[1].id, "abp-123");
        assert_eq!(cards[1].duration_secs, Some(45 * 60));
    }

    #[test]
    fn deduplicates_repeated_links_within_a_card() {
        let duplicated = format!("{LIST}{LIST}");
        assert_eq!(parse_list(&duplicated).len(), 2);
    }

    #[test]
    fn language_filter_excludes_other_subtrees() {
        let mixed = r#"
        <div class="thumbnail group">
          <a href="https://missav.ai/en/fc2-ppv-1" alt="x"><img data-src="a.jpg" alt="A"></a>
          <div><a href="https://missav.ai/en/fc2-ppv-1">A</a></div>
        </div>
        <div class="thumbnail group">
          <a href="https://missav.ai/cn/fc2-ppv-2" alt="y"><img data-src="b.jpg" alt="B"></a>
          <div><a href="https://missav.ai/cn/fc2-ppv-2">B</a></div>
        </div>"#;
        let only_cn = parse_list_for_language(mixed, Some("cn"));
        assert_eq!(only_cn.len(), 1);
        assert_eq!(only_cn[0].id, "fc2-ppv-2");
        assert_eq!(parse_list_for_language(mixed, None).len(), 2);
    }

    #[test]
    fn duration_parsing_variants() {
        assert_eq!(parse_duration("1:16:54"), Some(4614));
        assert_eq!(parse_duration("45:00"), Some(2700));
        assert_eq!(parse_duration("2:03"), Some(123));
        assert_eq!(parse_duration("4614"), None, "bare seconds are not a badge");
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("abc"), None);
    }

    #[test]
    fn video_id_extraction() {
        assert_eq!(
            video_id_from_url("https://missav.ai/cn/fc2-ppv-4968310").as_deref(),
            Some("fc2-ppv-4968310")
        );
        assert_eq!(
            video_id_from_url("https://missav.ai/fc2-ppv-4968310").as_deref(),
            Some("fc2-ppv-4968310")
        );
        assert_eq!(
            video_id_from_url("https://missav.ai/en/abp-123?x=1").as_deref(),
            Some("abp-123")
        );
        // Section pages are not videos.
        assert_eq!(video_id_from_url("https://missav.ai/cn/actresses"), None);
        assert_eq!(video_id_from_url("https://missav.ai/cn/vip"), None);
        assert_eq!(video_id_from_url("https://missav.ai/dm597/cn/fc2"), None);
        // Other hosts are ignored even if the path looks right.
        assert_eq!(video_id_from_url("https://example.com/cn/abp-123"), None);
    }

    #[test]
    fn language_is_read_from_the_path() {
        assert_eq!(
            language_of("https://missav.ai/cn/fc2-ppv-1").as_deref(),
            Some("cn")
        );
        assert_eq!(language_of("https://missav.ai/fc2-ppv-1"), None);
    }
}
