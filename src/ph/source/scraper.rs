//! Scraping Pornhub listings and video pages.
//!
//! Listing pages are server-rendered grids of
//! `li.pcVideoListItem` cards. The card's position is the order the site chose,
//! so it doubles as the ranking. Titles live in the cover's `title` attribute;
//! durations live in `var.duration`.
//!
//! Video pages publish the player configuration in a `flashvars_<id>` object.
//! It contains `video_title`, `video_duration`, the internal video id, and a
//! `mediaDefinitions` entry for the progressive MP4 endpoint. That endpoint
//! (`/video/get_media`) returns the actual MP4 variants with dimensions.

use anyhow::Context;
use dom_query::{Document, NodeRef, Selection};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::ph::config::Config;
use crate::ph::source::http::{Fetcher, Session};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoCard {
    /// View key from the URL (`ph...`), the dedup key.
    pub id: String,
    pub url: String,
    pub title: String,
    pub image_url: String,
    /// Runtime badge ("12:34") in seconds; `None` when the card omits it.
    pub duration_secs: Option<u64>,
    /// 1-based position in the listing.
    pub rank: Option<usize>,
    /// Internal numeric video id, used to verify a fetched video page.
    #[serde(default)]
    pub vid: Option<String>,
    /// The card advertises a higher-quality version (when the site exposes it).
    #[serde(default)]
    pub hd: bool,
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
    if path.rsplit('/').next()? != "view_video.php" {
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
    host == "pornhub.com" || host.ends_with(".pornhub.com")
}

/// A view key is a non-empty run of ASCII alphanumerics.
fn is_view_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 64 && key.chars().all(|c| c.is_ascii_alphanumeric())
}

/// Pure parser for a listing page.
pub fn parse_list(html: &str, site_base: &str) -> Vec<VideoCard> {
    let doc = Document::from(html);
    let mut cards: Vec<VideoCard> = Vec::new();
    let base = site_base.trim_end_matches('/');

    // Navigation, featured uploads, and related videos are separate from the
    // selected source's main listing and must not enter its download queue.
    let mut listing = doc.select("#recommendedListings, #mostRecentVideosSection, #showAllChanelVideos, #videoCategory, #videoSearchResult");
    if listing.is_empty() {
        listing = Selection::from(doc.root());
    }
    let mut nodes: Vec<_> = listing.select("li.pcVideoListItem").iter().collect();
    if nodes.is_empty() {
        nodes = listing
            .select("div.videoBox, li.videoBox, div.video-card")
            .iter()
            .collect();
    }

    for card in nodes {
        let vkey = card
            .attr("data-video-vkey")
            .map(|value| value.to_string())
            .filter(|value| is_view_key(value))
            .or_else(|| {
                card.select("a[href*=\"view_video.php?viewkey=\"]")
                    .iter()
                    .find_map(|anchor| {
                        anchor
                            .attr("href")
                            .and_then(|href| video_id_from_url(&absolute(href.as_ref(), base)))
                    })
            });
        let Some(id) = vkey else {
            continue;
        };
        if cards.iter().any(|card| card.id == id) {
            continue;
        }

        let href = card
            .select("a[href*=\"view_video.php?viewkey=\"]")
            .iter()
            .find_map(|anchor| {
                anchor
                    .attr("href")
                    .map(|href| absolute(href.as_ref(), base))
            })
            .unwrap_or_else(|| format!("{base}/view_video.php?viewkey={id}"));

        let title = card
            .select("a[title]")
            .iter()
            .find_map(|anchor| {
                anchor
                    .attr("title")
                    .map(|value| entity_decoded(value.as_ref()))
            })
            .filter(|value| !value.is_empty())
            .or_else(|| {
                card.select("img[alt]")
                    .iter()
                    .find_map(|img| img.attr("alt").map(|value| entity_decoded(value.as_ref())))
                    .filter(|value| !value.is_empty())
            })
            .or_else(|| {
                let text = entity_decoded(card.select("span.title").text().as_ref());
                (!text.is_empty()).then_some(text)
            })
            .unwrap_or_else(|| id.clone());

        let image_url = card
            .select("img")
            .iter()
            .find_map(|img| {
                img.attr("data-image")
                    .or_else(|| img.attr("data-src"))
                    .or_else(|| img.attr("src"))
                    .map(|value| absolute(value.as_ref(), base))
            })
            .unwrap_or_default();

        let duration_secs = card
            .select("var.duration, span.duration, .duration")
            .iter()
            .find_map(|node| parse_duration(node.text().as_ref()))
            .or_else(|| {
                card.attr("data-duration")
                    .and_then(|value| parse_duration(value.as_ref()))
            });

        let vid = card
            .attr("data-video-id")
            .map(|value| value.to_string())
            .filter(|value| !value.is_empty() && value.chars().all(|c| c.is_ascii_digit()));

        let hd = card
            .select(".quality, .hd-text-icon")
            .iter()
            .any(|node| node.text().to_ascii_lowercase().contains("hd"));

        cards.push(VideoCard {
            id,
            url: href,
            title,
            image_url,
            duration_secs,
            rank: Some(cards.len() + 1),
            vid,
            hd,
        });
    }

    cards
}

/// Resolve a possibly protocol-relative or site-relative URL.
fn absolute(url: &str, base: &str) -> String {
    let url = url.trim();
    if url.starts_with("http://") || url.starts_with("https://") {
        return url.to_string();
    }
    if let Some(rest) = url.strip_prefix("//") {
        return format!("https://{rest}");
    }
    if url.starts_with('/') {
        return format!("{base}{url}");
    }
    url.to_string()
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

/// One progressive media definition from the player configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaDefinition {
    pub format: String,
    pub height: Option<u64>,
    pub width: Option<u64>,
    pub url: String,
}

/// What a video page yields.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct VideoPage {
    pub title: String,
    pub duration_secs: Option<u64>,
    /// Internal numeric video id from `flashvars_<id>`.
    pub vid: Option<String>,
    /// The page says the account is signed in.
    pub logged_in: Option<bool>,
    pub media: Vec<MediaDefinition>,
}

/// Pure parser for a video page.
pub fn parse_video_page(html: &str) -> VideoPage {
    let flashvars = extract_flashvars(html);
    let title = flashvars
        .as_ref()
        .and_then(|value| value.get("video_title"))
        .and_then(Value::as_str)
        .map(entity_decoded)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            let doc = Document::from(html);
            let heading = entity_decoded(doc.select("h1").text().as_ref());
            (!heading.is_empty()).then_some(heading)
        })
        .unwrap_or_default();
    let duration_secs = flashvars
        .as_ref()
        .and_then(|value| value.get("video_duration"))
        .and_then(|value| value.as_u64().or_else(|| value.as_f64().map(|v| v as u64)));
    let vid = flashvars
        .as_ref()
        .and_then(|value| value.get("video_id"))
        .and_then(|value| {
            value
                .as_str()
                .map(str::to_string)
                .or_else(|| value.as_u64().map(|value| value.to_string()))
        })
        .or_else(|| flashvars_id(html));
    let logged_in = parse_logged_in(html);
    let media = flashvars
        .as_ref()
        .and_then(|value| value.get("mediaDefinitions"))
        .and_then(Value::as_array)
        .map(|items| parse_definitions(items))
        .unwrap_or_default();
    VideoPage {
        title,
        duration_secs,
        vid,
        logged_in,
        media,
    }
}

/// Parse an AJAX `/video/get_media` response.
pub fn parse_media_response(value: &Value) -> Vec<MediaDefinition> {
    let items = value
        .as_array()
        .map(Vec::as_slice)
        .or_else(|| {
            value
                .get("mediaDefinitions")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
        })
        .unwrap_or_default();
    parse_definitions(items)
        .into_iter()
        .filter(|definition| definition.format == "mp4")
        .collect()
}

fn parse_definitions(items: &[Value]) -> Vec<MediaDefinition> {
    items
        .iter()
        .filter_map(|item| {
            let format = item
                .get("format")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let url = item.get("videoUrl").and_then(Value::as_str)?.trim();
            if url.is_empty() {
                return None;
            }
            Some(MediaDefinition {
                format,
                height: item.get("height").and_then(json_u64),
                width: item.get("width").and_then(json_u64),
                url: url.to_string(),
            })
        })
        .collect()
}

fn json_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_f64().map(|value| value as u64))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

/// The page's `isLoggedInUser` flag, when it renders one.
pub fn parse_logged_in(html: &str) -> Option<bool> {
    static LOGGED_IN: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"var\s+isLoggedInUser\s*=\s*([01])").expect("valid regex")
    });
    LOGGED_IN.captures(html).map(|captures| &captures[1] == "1")
}

/// One-time fields the login form needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginForm {
    pub token: String,
    pub redirect: String,
    pub from: String,
    pub intended_action: String,
}

/// Parse the hidden fields of the site's login form.
pub fn parse_login_form(html: &str) -> Option<LoginForm> {
    let doc = Document::from(html);
    // The sign-in modal lives in a <template>. Its contents are a separate
    // document fragment, so ordinary descendant selectors cannot reach them.
    let templates = doc.select("template");
    let template_roots = templates.nodes().iter().filter_map(|template| {
        let id = template.query(|node| node.as_element()?.template_contents)??;
        Some(NodeRef::new(id, &doc.tree))
    });
    std::iter::once(doc.root())
        .chain(template_roots)
        .find_map(|root| {
            Selection::from(root)
                .select("form.js-loginFormModal, form.js-loginForm")
                .iter()
                .find_map(|form| {
                    let value = |name: &str| {
                        form.select(&format!("input[name=\"{name}\"]"))
                            .iter()
                            .find_map(|input| {
                                input
                                    .attr("value")
                                    .map(|value| entity_decoded(value.as_ref()))
                            })
                            .unwrap_or_default()
                    };
                    let token = value("token");
                    (!token.is_empty()).then(|| LoginForm {
                        token,
                        redirect: value("redirect"),
                        from: value("from"),
                        intended_action: value("intended_action"),
                    })
                })
        })
}

/// The numeric id in `var flashvars_<id> = {…}`.
fn flashvars_id(html: &str) -> Option<String> {
    static NAME: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"var\s+flashvars_(\d+)\s*=").expect("valid regex")
    });
    NAME.captures(html).map(|captures| captures[1].to_string())
}

/// Extract and parse the player configuration object.
fn extract_flashvars(html: &str) -> Option<Value> {
    static START: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"var\s+flashvars_\w+\s*=\s*").expect("valid regex")
    });
    let start = START.find(html)?.end();
    let object = balanced_object(&html[start..])?;
    serde_json::from_str(object).ok()
}

/// Slice one balanced JSON object, respecting strings and escapes.
fn balanced_object(text: &str) -> Option<&str> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (index, byte) in bytes.iter().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return text.get(..=index);
                }
            }
            _ => {}
        }
    }
    None
}

pub struct ListingPage {
    pub cards: Vec<VideoCard>,
    /// Unfiltered order supplies subscription boundaries even when filters
    /// exclude the previously seen upload.
    pub ids: Vec<String>,
    pub next_page: Option<usize>,
}

/// Follow the site's pagination, independently of how many cards survive filters.
pub fn parse_next_page(html: &str, listing_url: &str, page: usize) -> Option<usize> {
    let base = url::Url::parse(listing_url).ok()?;
    Document::from(html)
        .select(".page_next:not(.disabled) a[href], a[rel=\"next\"]")
        .iter()
        .find_map(|anchor| {
            let href = anchor.attr("href")?;
            let next = base.join(href.as_ref()).ok()?;
            if next.origin() != base.origin() || next.path() != base.path() {
                return None;
            }
            next.query_pairs()
                .find_map(|(key, value)| {
                    (key == "page")
                        .then(|| value.parse::<usize>().ok())
                        .flatten()
                })
                .filter(|next_page| *next_page > page)
        })
}

/// Fetch one page of the listing.
pub async fn fetch_popular(
    session: &Session,
    fetch: &Fetcher,
    cfg: &Config,
    page: usize,
) -> anyhow::Result<ListingPage> {
    let matcher = cfg.title_matcher()?;
    let url = cfg.popular_url(page);
    let referer = format!("{}/", cfg.site_base.trim_end_matches('/'));
    let mut html = session
        .ensure_text(&url, &referer, fetch)
        .await
        .with_context(|| format!("failed to fetch {url}"))?;
    // A long-lived session can expire between runs. When an account is
    // configured, a signed-out listing means the recommendations would silently
    // fall back to the public feed, so sign in once and fetch it again.
    if parse_logged_in(&html) == Some(false)
        && cfg.account().is_some()
        && cfg.request_cookie().is_none()
    {
        log::warn!("Pornhub listing is signed out; logging in again");
        session
            .relogin(fetch)
            .await
            .with_context(|| "Pornhub session expired and re-login failed")?;
        html = session
            .ensure_text(&url, &referer, fetch)
            .await
            .with_context(|| format!("failed to fetch {url} after re-login"))?;
    }
    let logged_in = parse_logged_in(&html);
    session.observe_login(fetch, logged_in)?;
    anyhow::ensure!(
        logged_in != Some(false) || cfg.account().is_none() || cfg.request_cookie().is_some(),
        "Pornhub listing is still signed out after re-login"
    );
    let mut cards = parse_list(&html, &cfg.site_base);
    if cards.is_empty() {
        anyhow::bail!("no videos parsed from {url} — the markup may have changed");
    }
    let ids = cards.iter().map(|card| card.id.clone()).collect();
    apply_filters(&mut cards, cfg, matcher.as_ref());
    Ok(ListingPage {
        cards,
        ids,
        next_page: parse_next_page(&html, &url, page),
    })
}

/// Shared listing filters: minimum duration and the title matcher.
fn apply_filters(cards: &mut Vec<VideoCard>, cfg: &Config, matcher: Option<&regex::Regex>) {
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
        .text(url, crate::ph::source::http::origin_of(url).as_deref())
        .await
        .with_context(|| format!("failed to fetch {url}"))?;
    let page = parse_video_page(&html);
    if page.media.is_empty() {
        anyhow::bail!("no media definitions found on {url} — the player markup may have changed");
    }
    Ok(page)
}
