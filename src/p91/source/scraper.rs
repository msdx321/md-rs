//! Scraping 91Porn listings and video pages.
//!
//! Listing pages are server-rendered grids of `div.well.well-sm.videos-text-align`
//! cards. The site always returns them in the order its category asks for, so
//! the card's position in the page *is* the ranking; there is no reliable view
//! count in the markup.
//!
//! Card shape (abridged):
//!
//! ```html
//! <div class="well well-sm videos-text-align">
//!   <a href="https://www.91porn.com/view_video.php?viewkey=1950234389&page=1&viewtype=basic">
//!     <div class="thumb-overlay" id="playvthumb_1235716">
//!       <img class="img-responsive" src="https://…/thumb/1235716.jpg">
//!       <div class="hd-text-icon">HD</div>
//!       <span class="duration">00:28:16</span>
//!     </div>
//!     <span class="video-title title-truncate m-t-5">…title…</span>
//!   </a>
//!   <span class="info">Added:</span> 20 d <br>
//! </div>
//! ```
//!
//! Video pages do not publish the player source directly. They call
//! `document.write(strencode2("%3c%73%6f%75%72%63%65…"))`, and `strencode2` is
//! a heavily obfuscated alias for `unescape`. Decoding the literal as data
//! yields the real `<source src='…'>` tag, so no JavaScript engine is needed.

use anyhow::Context;
use dom_query::Document;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::p91::config::Config;
use crate::p91::source::http::Fetcher;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoCard {
    /// View key taken from the URL (`1950234389`), the dedup key.
    pub id: String,
    pub url: String,
    pub title: String,
    pub image_url: String,
    /// Runtime badge ("00:28:16") in seconds; `None` when the card omits it.
    pub duration_secs: Option<u64>,
    /// 1-based position in the listing, which the site already sorted.
    pub rank: Option<usize>,
    /// Internal video id from the card's `playvthumb_<id>`, used to verify that
    /// the page we fetched is the video the listing actually pointed at.
    #[serde(default)]
    pub vid: Option<String>,
    /// The card advertises a higher-quality version.
    #[serde(default)]
    pub hd: bool,
    /// The card is flagged as an original upload.
    #[serde(default)]
    pub original: bool,
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

/// Extract the view key from a video page URL.
pub fn video_id_from_url(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let host = parsed.host_str()?.to_ascii_lowercase();
    if !matches!(parsed.scheme(), "http" | "https") || !is_site_host(&host) {
        return None;
    }
    let path = parsed.path().trim_end_matches('/');
    if !matches!(
        path.rsplit('/').next()?,
        "view_video.php" | "view_video_hd.php"
    ) {
        return None;
    }
    let key = parsed
        .query_pairs()
        .find(|(name, _)| name == "viewkey")
        .map(|(_, value)| value.into_owned())?;
    is_view_key(&key).then_some(key)
}

fn is_site_host(host: &str) -> bool {
    let host = host.trim_start_matches("www.");
    host == "91porn.com" || host.ends_with(".91porn.com")
}

/// A view key is a non-empty run of ASCII alphanumerics.
fn is_view_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 64 && key.chars().all(|c| c.is_ascii_alphanumeric())
}

/// Pure parser for a listing page.
pub fn parse_list(html: &str) -> Vec<VideoCard> {
    let doc = Document::from(html);
    let mut cards: Vec<VideoCard> = Vec::new();

    for card in doc.select("div.well.well-sm.videos-text-align").iter() {
        // Every detail link for this card; the cover and headline share it.
        let mut url = None;
        let mut id = None;
        for anchor in card.select("a[href*=\"view_video.php?viewkey=\"]").iter() {
            let Some(href) = anchor.attr("href").map(|v| v.to_string()) else {
                continue;
            };
            let href = absolute(&href);
            let Some(key) = video_id_from_url(&href) else {
                continue;
            };
            url = Some(href);
            id = Some(key);
            break;
        }
        let (Some(url), Some(id)) = (url, id) else {
            continue;
        };
        // Desktop and mobile layouts repeat the same card; keep the first.
        if cards.iter().any(|c| c.id == id) {
            continue;
        }

        let mut title = entity_decoded(card.select("span.video-title").text().as_ref());
        if title.is_empty() {
            title = entity_decoded(
                &card
                    .select("img")
                    .iter()
                    .next()
                    .and_then(|img| img.attr("alt").map(|v| v.to_string()))
                    .unwrap_or_default(),
            );
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
            .select("span.duration")
            .iter()
            .find_map(|span| parse_duration(span.text().as_ref()));

        let vid = card
            .select("div.thumb-overlay")
            .iter()
            .find_map(|node| node.attr("id").map(|v| v.to_string()))
            .and_then(|id| id.strip_prefix("playvthumb_").map(str::to_string))
            .filter(|id| !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()));

        // Listing pages embed a second, stale copy of the grid whose card ids
        // and thumbnails disagree. Those entries resolve to unrelated videos,
        // so a card is only trusted when its own two ids agree.
        if vid.is_none() || vid.as_deref() != thumb_id(&image_url).as_deref() {
            continue;
        }

        cards.push(VideoCard {
            id,
            url: absolute(&url),
            title,
            image_url: absolute(&image_url),
            duration_secs,
            rank: Some(cards.len() + 1),
            vid,
            hd: card.select("div.hd-text-icon").iter().next().is_some(),
            original: card
                .select("div.original-text-icon")
                .iter()
                .next()
                .is_some(),
        });
    }

    cards
}

/// Resolve a possibly protocol-relative or site-relative URL.
fn absolute(url: &str) -> String {
    let url = url.trim();
    if url.starts_with("http://") || url.starts_with("https://") {
        return url.to_string();
    }
    if let Some(rest) = url.strip_prefix("//") {
        return format!("https://{rest}");
    }
    if url.starts_with('/') {
        return format!("https://www.91porn.com{url}");
    }
    url.to_string()
}

/// Numeric id from a `…/thumb/<id>.jpg` cover URL.
fn thumb_id(url: &str) -> Option<String> {
    static THUMB: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"/thumb/(\d+)\.").expect("valid regex"));
    THUMB.captures(url).map(|c| c[1].to_string())
}

/// Trim whitespace and decode the HTML entities the site writes into titles.
pub fn entity_decoded(text: &str) -> String {
    static ENTITY: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"&(#[0-9]+|#[xX][0-9a-fA-F]+|[a-zA-Z]+);").expect("valid regex")
    });
    ENTITY
        .replace_all(text, |captures: &regex::Captures<'_>| {
            let body = &captures[1];
            let decoded =
                if let Some(hex) = body.strip_prefix("#x").or_else(|| body.strip_prefix("#X")) {
                    u32::from_str_radix(hex, 16).ok().and_then(char::from_u32)
                } else if let Some(decimal) = body.strip_prefix('#') {
                    decimal.parse::<u32>().ok().and_then(char::from_u32)
                } else {
                    match body {
                        "amp" => Some('&'),
                        "lt" => Some('<'),
                        "gt" => Some('>'),
                        "quot" => Some('"'),
                        "apos" => Some('\''),
                        "nbsp" => Some(' '),
                        _ => None,
                    }
                };
            match decoded {
                Some(ch) => ch.to_string(),
                None => captures[0].to_string(),
            }
        })
        .trim()
        .to_string()
}

/// What a video page yields: a title and one progressive media URL.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct VideoPage {
    pub title: String,
    pub source_url: Option<String>,
    /// Internal video id the site rendered (`<div id=VID>…</div>`).
    pub vid: Option<String>,
}

/// Pure parser for a video page.
pub fn parse_video_page(html: &str) -> VideoPage {
    let doc = Document::from(html);

    let mut title = String::new();
    for heading in doc.select("div#videodetails h4").iter() {
        let text = entity_decoded(heading.text().as_ref());
        if !text.is_empty() && !text.starts_with("Video Information") {
            title = text;
            break;
        }
    }
    if title.is_empty() {
        title = entity_decoded(doc.select("title").text().as_ref());
    }

    VideoPage {
        title,
        source_url: extract_source_url(html),
        vid: page_vid(&doc),
    }
}

/// The `VID` element the site renders into every video page.
fn page_vid(doc: &Document) -> Option<String> {
    let vid = doc.select("div#VID").text().to_string().trim().to_string();
    (!vid.is_empty() && vid.chars().all(|c| c.is_ascii_digit())).then_some(vid)
}

/// Recover the `<source>` URL hidden behind `strencode2`.
pub fn extract_source_url(html: &str) -> Option<String> {
    if let Some(decoded) = decode_strencode2_calls(html)
        && let Some(source) = first_source(&decoded)
    {
        return Some(source);
    }
    // Older pages embed the tag directly, but the same page also carries a
    // commented-out placeholder, so comments are removed before matching.
    first_source(&strip_comments(html))
}

/// Decode every `document.write(strencode2("…"))` payload on the page.
fn decode_strencode2_calls(html: &str) -> Option<String> {
    static CALL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(
            r#"(?s)document\.write\(\s*strencode2\(\s*(?:"((?:\\.|[^"\\])*)"|'((?:\\.|[^'\\])*)')\s*\)\s*\)"#,
        )
        .expect("valid regex")
    });
    let mut decoded = String::new();
    for captures in CALL.captures_iter(html) {
        let payload = captures
            .get(1)
            .or_else(|| captures.get(2))
            .map(|m| m.as_str())
            .unwrap_or_default();
        decoded.push_str(&unescape(payload));
        decoded.push('\n');
    }
    (!decoded.trim().is_empty()).then_some(decoded)
}

/// JavaScript's `unescape`: `%xx` byte escapes plus `%uXXXX` code units.
///
/// Unlike `decodeURIComponent` it never treats `+` as a space, and invalid
/// escapes are left as literal text.
pub fn unescape(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'%' {
            out.push(bytes[i]);
            i += 1;
            continue;
        }
        // `%uXXXX` takes priority; a plain `%xx` otherwise.
        if matches!(bytes.get(i + 1), Some(b'u' | b'U'))
            && let Some(code) = bytes.get(i + 2..i + 6).and_then(hex_value)
        {
            if let Some(ch) = char::from_u32(code) {
                let mut buf = [0u8; 4];
                out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
            }
            i += 6;
            continue;
        }
        if let Some(byte) = bytes.get(i + 1..i + 3).and_then(hex_value) {
            out.push(byte as u8);
            i += 3;
            continue;
        }
        out.push(b'%');
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parse a run of hexadecimal ASCII digits.
fn hex_value(digits: &[u8]) -> Option<u32> {
    if digits.is_empty() {
        return None;
    }
    digits.iter().try_fold(0u32, |acc, byte| {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return None,
        };
        Some(acc * 16 + u32::from(digit))
    })
}

fn strip_comments(html: &str) -> String {
    static COMMENT: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"(?s)<!--.*?-->").expect("valid regex"));
    COMMENT.replace_all(html, "").into_owned()
}

/// First `<source src="…">` in an HTML fragment.
fn first_source(html: &str) -> Option<String> {
    static SOURCE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"(?i)<source[^>]*\bsrc\s*=\s*(?:"([^"]+)"|'([^']+)')"#)
            .expect("valid regex")
    });
    let captures = SOURCE.captures(html)?;
    let url = captures
        .get(1)
        .or_else(|| captures.get(2))?
        .as_str()
        .trim()
        .to_string();
    (!url.is_empty()).then_some(url)
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
    let mut cards = parse_list(&html);
    if cards.is_empty() {
        anyhow::bail!("no videos parsed from {url} — the markup may have changed");
    }
    apply_filters(&mut cards, cfg, matcher.as_ref());
    Ok(cards)
}

/// Shared listing filters: HD-only, minimum duration, and the title matcher.
/// Applied after parsing so an empty result is distinguished from a markup change.
fn apply_filters(cards: &mut Vec<VideoCard>, cfg: &Config, matcher: Option<&regex::Regex>) {
    if cfg.hd_only {
        cards.retain(|card| card.hd);
    }
    if cfg.min_duration_secs > 0.0 {
        // Unknown durations stay eligible rather than being silently dropped.
        cards.retain(|card| {
            card.duration_secs
                .is_none_or(|secs| secs as f64 >= cfg.min_duration_secs)
        });
    }
    if let Some(matcher) = matcher {
        cards.retain(|card| matcher.is_match(&card.title));
    }
}

/// Fetch and parse one video page.
pub async fn fetch_video_page(fetch: &Fetcher, url: &str) -> anyhow::Result<VideoPage> {
    let html = fetch
        .text(url, crate::p91::source::http::origin_of(url).as_deref())
        .await
        .with_context(|| format!("failed to fetch {url}"))?;
    let mut page = parse_video_page(&html);
    if let Some(source) = &page.source_url {
        let source = Url::parse(url)?.join(&entity_decoded(source))?;
        anyhow::ensure!(
            matches!(source.scheme(), "http" | "https"),
            "invalid media URL scheme"
        );
        page.source_url = Some(source.into());
    }
    if page.source_url.is_none() {
        anyhow::bail!("no video source found on {url} — the player markup may have changed");
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIST: &str = r#"
    <div class="row">
      <div class="well well-sm videos-text-align">
        <a href="https://www.91porn.com/view_video.php?viewkey=1950234389&page=1&viewtype=basic&category=top">
          <div class="thumb-overlay" id="playvthumb_1235716">
            <img class="img-responsive" src="https://cdn.example/thumb/1235716.jpg" />
            <div class="original-text-icon">91</div>
            <span class="duration">00:28:16</span>
          </div>
          <span class="video-title title-truncate m-t-5">First title</span>
        </a>
        <span class="info">Added:</span> 20 d
      </div>
      <div class="well well-sm videos-text-align">
        <a href="https://www.91porn.com/view_video.php?viewkey=9afa3b490bf4da774963&viewtype=basic">
          <div class="thumb-overlay" id="playvthumb_1239647">
            <img class="img-responsive" src="https://cdn.example/thumb/1239647.jpg" />
            <div class="hd-text-icon">HD</div>
            <span class="duration">00:08:59</span>
          </div>
          <span class="video-title">Second title</span>
        </a>
      </div>
      <div class="well well-sm videos-text-align">
        <a href="https://www.91porn.com/view_video.php?viewkey=1950234389&page=1">
          <img class="img-responsive" src="https://cdn.example/thumb/dup.jpg" />
          <span class="video-title">Duplicate of the first</span>
        </a>
      </div>
      <div class="well well-sm videos-text-align">
        <a href="https://www.91porn.com/view_video.php?viewkey=1874104404&viewtype=basic">
          <div class="thumb-overlay" id="playvthumb_9999999">
            <img class="img-responsive" src="https://cdn.example/thumb/1230000.jpg" />
            <div class="hd-text-icon">HD</div>
          </div>
          <span class="video-title">Stale duplicate block</span>
        </a>
      </div>
      <div class="other"><a href="https://www.91porn.com/v.php?category=top">Not a video</a></div>
    </div>"#;

    #[test]
    fn parses_synthetic_cards() {
        let cards = parse_list(LIST);
        assert_eq!(
            cards.len(),
            2,
            "the duplicate, stale and non-video blocks must be dropped"
        );
        assert_eq!(cards[0].id, "1950234389");
        assert_eq!(cards[0].title, "First title");
        assert_eq!(cards[0].duration_secs, Some(28 * 60 + 16));
        assert_eq!(cards[0].rank, Some(1));
        assert_eq!(cards[0].vid.as_deref(), Some("1235716"));
        assert!(cards[0].original);
        assert!(!cards[0].hd);
        assert_eq!(cards[1].id, "9afa3b490bf4da774963");
        assert_eq!(cards[1].rank, Some(2));
        assert!(cards[1].hd);
        assert!(!cards[1].original);
    }

    #[test]
    fn filters_listings_by_quality_and_duration() {
        let hd_only = Config {
            hd_only: true,
            ..Config::default()
        };
        let mut cards = parse_list(LIST);
        apply_filters(&mut cards, &hd_only, None);
        assert_eq!(cards.len(), 1);
        assert!(cards[0].hd);

        let long = Config {
            min_duration_secs: 600.0,
            ..Config::default()
        };
        let mut cards = parse_list(LIST);
        apply_filters(&mut cards, &long, None);
        assert_eq!(cards.len(), 1, "the 8m59s card must be filtered out");
        assert_eq!(cards[0].duration_secs, Some(28 * 60 + 16));

        let filtered = Config {
            title_filter: "second".into(),
            ..Config::default()
        };
        let matcher = filtered.title_matcher().unwrap();
        let mut cards = parse_list(LIST);
        apply_filters(&mut cards, &filtered, matcher.as_ref());
        assert_eq!(cards.len(), 1);
        assert_eq!(cards[0].title, "Second title");
    }

    #[test]
    fn drops_cards_whose_ids_disagree() {
        // The live listing embeds a second grid whose video ids and thumbnails
        // disagree; those cards resolve to unrelated videos.
        let cards = parse_list(LIST);
        assert!(
            cards.iter().all(|card| card.id != "1874104404"),
            "the stale duplicate block leaked through"
        );
    }

    #[test]
    fn video_id_extraction() {
        assert_eq!(
            video_id_from_url("https://www.91porn.com/view_video.php?viewkey=1950234389")
                .as_deref(),
            Some("1950234389")
        );
        assert_eq!(
            video_id_from_url(
                "https://91porn.com/view_video_hd.php?viewkey=d6e29b733398775274a3&x=1"
            )
            .as_deref(),
            Some("d6e29b733398775274a3")
        );
        assert_eq!(
            video_id_from_url("https://www.91porn.com/v.php?category=top"),
            None
        );
        assert_eq!(
            video_id_from_url("https://example.com/view_video.php?viewkey=1"),
            None
        );
        assert_eq!(
            video_id_from_url("https://www.91porn.com/view_video.php?viewkey=bad%20key"),
            None
        );
    }

    #[test]
    fn decodes_strencode2_payloads() {
        let html = r#"<script>document.write(strencode2("%3c%73%6f%75%72%63%65%20%73%72%63%3d%27%68%74%74%70%73%3a%2f%2f%6c%61%2e%62%74%63%36%32%30%2e%63%6f%6d%2f%2f%6d%70%34%33%2f%31%32%33%39%33%39%37%2e%6d%70%34%3f%73%74%3d%61%62%63%26%65%3d%31%26%66%3d%64%65%66%27%20%74%79%70%65%3d%27%76%69%64%65%6f%2f%6d%70%34%27%3e"));</script>"#;
        let page = parse_video_page(html);
        assert_eq!(
            page.source_url.as_deref(),
            Some("https://la.btc620.com//mp43/1239397.mp4?st=abc&e=1&f=def")
        );
    }

    #[test]
    fn ignores_the_commented_out_placeholder() {
        let html = r#"<video>
          <!-- <source src="https://ccm.91p52.com/358999.mp4?st=decoy" type='video/mp4'> -->
          <source src="/real/video.mp4" type='video/mp4'>
        </video>"#;
        assert_eq!(extract_source_url(html).as_deref(), Some("/real/video.mp4"));
    }

    #[test]
    fn reads_the_title_from_the_heading() {
        let html = r#"<div id="videodetails"><h4 class="x">Real title here</h4></div>
          <div id="videodetails"><h4>Video Information<br></h4></div>"#;
        assert_eq!(parse_video_page(html).title, "Real title here");
    }

    #[test]
    fn unescape_handles_bytes_and_code_units() {
        assert_eq!(unescape("%3c%3e"), "<>");
        assert_eq!(unescape("%u4e2d%u6587"), "中文");
        assert_eq!(unescape("100%"), "100%");
        assert_eq!(unescape("%zz"), "%zz");
        assert_eq!(unescape("a+b"), "a+b", "plus is not a space for unescape");
    }

    #[test]
    fn absolute_resolves_relative_urls() {
        assert_eq!(absolute("//cdn.example/a.jpg"), "https://cdn.example/a.jpg");
        assert_eq!(
            absolute("/thumb/1.jpg"),
            "https://www.91porn.com/thumb/1.jpg"
        );
        assert_eq!(absolute("https://x/a.jpg"), "https://x/a.jpg");
    }

    #[test]
    fn decodes_html_entities_in_titles() {
        assert_eq!(entity_decoded("a &quot;b&quot; &#39;c&#39;"), "a \"b\" 'c'");
        assert_eq!(entity_decoded("x &amp; y &lt;z&gt;"), "x & y <z>");
        assert_eq!(entity_decoded("&#x4e2d;&#25991;"), "中文");
        assert_eq!(entity_decoded("  spaced  "), "spaced");
        assert_eq!(entity_decoded("&unknown; stays"), "&unknown; stays");
    }
}
