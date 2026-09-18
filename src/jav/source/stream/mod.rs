//! Turning a video page into something downloadable.
//!
//! MissAV serves the playlist straight from the page: the player's script is
//! packed with the Dean Edwards packer, and decoding it as data yields a
//! `https://surrit.com/<uuid>/playlist.m3u8` master playlist. There are no rotating
//! third-party mirrors to chase, so the old SupJav mirror indirection is gone.

pub mod m3u8;

use std::error::Error;
use std::sync::LazyLock;
use std::time::Duration;

use dom_query::Document;
use url::Url;

use crate::jav::source::http::{Fetcher, MediaRejected, validate_media_body};

use m3u8::{M3u8Info, parse_master_variants, parse_media_m3u8, select_audio_uri, select_variant};

/// What kind of payload a resolved stream contains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Hls,
}

#[derive(Debug, Clone)]
pub struct ResolvedStream {
    pub title: String,
    pub playlist: M3u8Info,
    pub kind: StreamKind,
}

/// Title plus the playlist scraped out of a video page.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct VideoPage {
    pub title: String,
    /// Master playlist advertised by the player, if the page carried one.
    pub playlist_url: Option<String>,
}

/// Pure parser for a MissAV video page.
pub fn parse_video_page(html: &str) -> VideoPage {
    let doc = Document::from(html);
    let h1 = doc.select("h1").text().to_string().trim().to_string();
    let title = if !h1.is_empty() {
        h1
    } else {
        let og = doc
            .select("meta[property=\"og:title\"]")
            .iter()
            .next()
            .and_then(|m| m.attr("content").map(|v| v.to_string()))
            .unwrap_or_default();
        if !og.trim().is_empty() {
            og.trim().to_string()
        } else {
            doc.select("title").text().to_string().trim().to_string()
        }
    };

    VideoPage {
        title,
        playlist_url: extract_m3u8_url(html),
    }
}

/// Fetch a video page and pull its playlist out.
pub async fn fetch_video_page(fetch: &Fetcher, page_url: &str) -> anyhow::Result<VideoPage> {
    let html = fetch.text(page_url, Some(&origin_of(page_url))).await?;
    let page = parse_video_page(&html);
    if page.playlist_url.is_none() {
        anyhow::bail!(
            "no playlist found on the page (title: {:?}) — the player markup may have changed",
            page.title
        );
    }
    Ok(page)
}

/// The site origin a page belongs to, used as the `Referer` for its media.
pub fn origin_of(url: &str) -> String {
    Url::parse(url)
        .ok()
        .map(|u| format!("{}/", u.origin().ascii_serialization()))
        .unwrap_or_else(|| "https://missav.ai/".to_string())
}

/// Resolve a video page to a playable master/variant playlist.
///
/// The playlist is fetched once to learn its variants and duration, and the
/// variant matching `resolution` is returned. A stream shorter than
/// `min_duration_secs` is still returned when it is the only one available,
/// with a warning, rather than failing the whole download.
pub async fn resolve_stream(
    fetch: &Fetcher,
    page_url: &str,
    resolution: &str,
    min_duration_secs: f64,
) -> anyhow::Result<ResolvedStream> {
    let page = fetch_video_page(fetch, page_url).await?;
    let title = page.title.clone();
    let playlist_url = page
        .playlist_url
        .expect("fetch_video_page guarantees a playlist");

    let info = resolve_playlist(fetch, page_url, &playlist_url, resolution)
        .await
        .map_err(|e| anyhow::anyhow!("cannot read the playlist {playlist_url}: {e}"))?;

    log::info!(
        "playlist: {} segments, {:.1} min, resolution {:?}",
        info.segments.len(),
        info.total_duration / 60.0,
        info.variant.resolution
    );
    if info.total_duration < min_duration_secs {
        log::warn!(
            "stream is shorter than the {:.0}s threshold ({:.1} min); using it anyway",
            min_duration_secs,
            info.total_duration / 60.0
        );
    }

    Ok(ResolvedStream {
        title,
        playlist: info,
        kind: StreamKind::Hls,
    })
}

/// Find a `.m3u8` URL, including inside a packed player script.
pub fn extract_m3u8_url(body: &str) -> Option<String> {
    static PLAY: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r#"urlPlay[\s=:'"]+(https?://[^\s'"\\]+\.m3u8[^\s'"\\]*)"#)
            .expect("valid regex")
    });
    if let Some(caps) = PLAY.captures(body) {
        return caps.get(1).map(|m| m.as_str().to_string());
    }
    static ANY: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r#"(https?://[^\s'"\\]+\.m3u8[^\s'"\\]*)"#).expect("valid regex")
    });
    if let Some(url) = ANY
        .captures(body)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
    {
        return Some(url);
    }

    // MissAV's player hides the playlist in a packed script; prefer its
    // master playlist over the quality variants beside it.
    let packed = decode_packed_scripts(body);
    static DEDICATED: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r#"https?://[^\s'"\\]+/playlist\.m3u8[^\s'"\\]*"#).expect("valid regex")
    });
    if let Some(url) = DEDICATED.find(&packed).map(|m| m.as_str().to_string()) {
        return Some(url);
    }
    if let Some(url) = ANY
        .captures(&packed)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
    {
        return Some(url);
    }

    None
}

/// Decode every Dean Edwards packed script in `body`, concatenated.
///
/// The packer hides string literals behind a dictionary indexed in an arbitrary
/// radix (`'…'.split('|')`), so the radix has to be read from the payload rather
/// than assumed: SupJav's mirrors use base 36, MissAV's player uses base 15.
///
/// The dictionary is treated purely as data — received code is never executed.
fn decode_packed_scripts(body: &str) -> String {
    static PACKED: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(
            r#"\}\(\s*'((?:\\.|[^'\\])*)'\s*,\s*(\d+)\s*,\s*\d+\s*,\s*'((?:\\.|[^'\\])*)'\.split\('\|'\)"#,
        )
        .expect("valid regex")
    });
    let mut out = String::new();
    for captures in PACKED.captures_iter(body) {
        let payload = unescape_packed_string(&captures[1]);
        let Some(radix) = captures[2]
            .parse::<u32>()
            .ok()
            .filter(|r| (2..=36).contains(r))
        else {
            continue;
        };
        let dictionary = unescape_packed_string(&captures[3]);
        out.push_str(&unpack_script(&payload, radix, &dictionary));
        out.push('\n');
    }
    out
}

/// Substitute dictionary references in a packed payload.
///
/// Letters are digits above base 10 too: in MissAV's base-15 player, `d`
/// indexes `playlist` and `a` indexes `720p`.
pub fn unpack_script(payload: &str, radix: u32, dictionary: &str) -> String {
    let words: Vec<&str> = dictionary.split('|').collect();
    static WORD: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"\b[0-9a-z]+\b").expect("valid regex"));
    WORD.replace_all(payload, |token: &regex::Captures<'_>| {
        usize::from_str_radix(&token[0], radix)
            .ok()
            .and_then(|index| words.get(index).copied())
            .filter(|value| !value.is_empty())
            .unwrap_or(&token[0])
            .to_string()
    })
    .into_owned()
}

fn unescape_packed_string(value: &str) -> String {
    let mut decoded = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') => decoded.push('\n'),
                Some('r') => decoded.push('\r'),
                Some('t') => decoded.push('\t'),
                Some(escaped) => decoded.push(escaped),
                None => decoded.push('\\'),
            }
        } else {
            decoded.push(ch);
        }
    }
    decoded
}

/// Fetch and parse a playlist, descending through master playlists and
/// sub-playlist indirections.
pub async fn resolve_playlist(
    fetch: &Fetcher,
    origin_url: &str,
    playlist_url: &str,
    resolution: &str,
) -> Result<M3u8Info, Box<dyn Error>> {
    Box::pin(resolve_recursive(
        fetch,
        origin_url,
        playlist_url,
        resolution,
        0,
    ))
    .await
}

/// Boxed future used by the recursive playlist resolver.
type ResolveFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<M3u8Info, Box<dyn Error>>> + Send + 'a>,
>;

fn resolve_recursive<'a>(
    fetch: &'a Fetcher,
    origin_url: &'a str,
    playlist_url: &'a str,
    resolution: &'a str,
    depth: usize,
) -> ResolveFuture<'a> {
    Box::pin(async move {
        if depth > 5 {
            return Err("m3u8 recursion limit exceeded".into());
        }

        let mut last_err = String::new();
        let mut text = String::new();
        let mut effective_url = playlist_url.to_string();
        for attempt in 1..=3 {
            match fetch.media_response(playlist_url, origin_url, None).await {
                Ok(resp) => {
                    effective_url = resp.uri().to_string();
                    match resp.text().await {
                        Ok(t) => {
                            validate_media_body(playlist_url, t.as_bytes())?;
                            if !t
                                .trim_start_matches('\u{feff}')
                                .trim_start()
                                .starts_with("#EXTM3U")
                            {
                                return Err(format!(
                                    "invalid HLS playlist at {playlist_url}: missing #EXTM3U"
                                )
                                .into());
                            }
                            text = t;
                            break;
                        }
                        Err(e) => last_err = e.to_string(),
                    }
                }
                Err(e) if e.is::<MediaRejected>() => return Err(e.into()),
                Err(e) => last_err = e.to_string(),
            }
            if attempt < 3 {
                tokio::time::sleep(Duration::from_millis(300 * attempt as u64)).await;
            }
        }
        if text.is_empty() {
            return Err(last_err.into());
        }

        let base_url = Url::parse(&effective_url)?;

        // 1. Master playlist.
        if text.contains("#EXT-X-STREAM-INF") {
            let variants = parse_master_variants(&text, &base_url);
            if variants.is_empty() {
                return Err("master playlist contains no variants".into());
            }
            let mut resolved = Vec::new();
            for variant in variants {
                match Box::pin(resolve_recursive(
                    fetch,
                    origin_url,
                    &variant.uri,
                    resolution,
                    depth + 1,
                ))
                .await
                {
                    Ok(mut info) => {
                        info.variant.resolution = info.variant.resolution.or(variant.resolution);
                        info.variant.bandwidth = info.variant.bandwidth.or(variant.bandwidth);
                        info.variant.audio_group = variant.audio_group;
                        resolved.push(info);
                    }
                    Err(e) => {
                        last_err = format!("variant {}: {e}", variant.uri);
                        log::debug!("{last_err}");
                    }
                }
            }
            if resolved.is_empty() {
                return Err(format!("no variant playlist could be resolved: {last_err}").into());
            }

            // Variant choice is purely about resolution; duration is reported
            // to the caller, which decides whether to warn or reject.
            let list: Vec<m3u8::Variant> =
                resolved.iter().map(|info| info.variant.clone()).collect();
            let index = select_variant(&list, resolution)
                .and_then(|best| {
                    resolved
                        .iter()
                        .position(|info| info.variant.uri == best.uri)
                })
                .unwrap_or(0);
            let mut info = resolved.swap_remove(index);
            let audio_uri = info
                .variant
                .audio_group
                .as_deref()
                .map(|group| select_audio_uri(&text, &base_url, group))
                .transpose()?
                .flatten();
            if let Some(uri) = audio_uri {
                let audio =
                    resolve_recursive(fetch, origin_url, &uri, resolution, depth + 1).await?;
                if audio.segments.is_empty() || audio.audio.is_some() {
                    return Err("invalid external HLS audio playlist".into());
                }
                info.audio = Some(Box::new(audio));
            }
            return Ok(info);
        }

        // 2. Media playlist.
        if text.contains("#EXTINF:") {
            return parse_media_m3u8(&text, &base_url);
        }

        // 3. Indirection: the body only lists sub-playlists.
        let mut sub_urls = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if line.split('?').next().unwrap_or(line).contains(".m3u8")
                && let Ok(resolved) = base_url.join(line)
            {
                sub_urls.push(resolved.to_string());
            }
        }
        if !sub_urls.is_empty() {
            let mut infos = Vec::new();
            for sub in sub_urls {
                if let Ok(info) = Box::pin(resolve_recursive(
                    fetch,
                    origin_url,
                    &sub,
                    resolution,
                    depth + 1,
                ))
                .await
                {
                    infos.push(info);
                }
            }
            if let Some(best) = infos.into_iter().max_by(|a, b| {
                a.total_duration
                    .partial_cmp(&b.total_duration)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }) {
                return Ok(best);
            }
        }

        parse_media_m3u8(&text, &base_url)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_title_and_playlist() {
        let page = parse_video_page(
            r#"<html><body><h1>Some Title</h1>
               <script>var src='https://cdn.example.com/hls/master.m3u8';</script>
               </body></html>"#,
        );
        assert_eq!(page.title, "Some Title");
        assert_eq!(
            page.playlist_url.as_deref(),
            Some("https://cdn.example.com/hls/master.m3u8")
        );
    }

    #[test]
    fn falls_back_to_og_title_then_document_title() {
        let og = parse_video_page(
            r#"<html><head>
                 <meta property="og:title" content="OG Title">
                 <title>Doc Title</title>
               </head><body></body></html>"#,
        );
        assert_eq!(og.title, "OG Title");

        let doc = parse_video_page("<html><head><title>Doc Title</title></head></html>");
        assert_eq!(doc.title, "Doc Title");
        assert!(doc.playlist_url.is_none());
    }

    #[test]
    fn extracts_m3u8_from_urlplay_assignment() {
        let body = r#"var player = {urlPlay: 'https://cdn.example.com/hls/master.m3u8?token=1'};"#;
        assert_eq!(
            extract_m3u8_url(body).as_deref(),
            Some("https://cdn.example.com/hls/master.m3u8?token=1")
        );
    }

    #[test]
    fn extracts_m3u8_from_escaped_body() {
        let body = r#"{"file":"https:\/\/cdn.example.com\/a\/index.m3u8"}"#.replace("\\/", "/");
        assert_eq!(
            extract_m3u8_url(&body).as_deref(),
            Some("https://cdn.example.com/a/index.m3u8")
        );
    }

    #[test]
    fn no_m3u8_returns_none() {
        assert_eq!(extract_m3u8_url("<html>nothing here</html>"), None);
    }

    /// The real player page captured from `https://missav.ai/cn/fc2-ppv-4968310`.
    const LIVE_VIDEO_PAGE: &str = include_str!("../../fixtures/missav_video.html");

    #[test]
    fn extracts_the_playlist_from_the_live_missav_page() {
        let url = extract_m3u8_url(LIVE_VIDEO_PAGE).expect("playlist should be found");
        assert!(
            url.starts_with("https://surrit.com/"),
            "unexpected host: {url}"
        );
        assert!(
            url.ends_with("/playlist.m3u8"),
            "expected the dedicated playlist: {url}"
        );
    }

    #[test]
    fn unpack_script_handles_base_15() {
        // The exact shape MissAV's player ships: radix 15, dictionary indexes
        // written in base 15, including letter digits.
        let payload = r"e='8://7.6/5-4-3-2-1/d.0';c='8://7.6/5-4-3-2-1/a/9.0';";
        let dictionary = "m3u8|bd4889d38713|a2d5|48c7|c1f8|7aab6fb0|com|surrit|https|video|720p|source1280|source842|playlist|source";
        let decoded = unpack_script(payload, 15, dictionary);
        assert_eq!(
            decoded,
            r"source='https://surrit.com/7aab6fb0-c1f8-48c7-a2d5-bd4889d38713/playlist.m3u8';source842='https://surrit.com/7aab6fb0-c1f8-48c7-a2d5-bd4889d38713/720p/video.m3u8';"
        );
    }

    #[test]
    fn unpack_script_handles_base_36() {
        // At base 36 letters are digits, so tokens like 'q' are references too.
        let words: Vec<String> = (0..26).map(|i| ((b'a' + i) as char).to_string()).collect();
        let mut dictionary = words.join("|");
        dictionary.push_str("|https|com|example|m3u8");
        // Indices 26..29 in base 36 are q, r, s, t.
        let payload = "q.r/s.t";
        assert_eq!(
            unpack_script(payload, 36, &dictionary),
            "https.com/example.m3u8"
        );
    }

    #[test]
    fn unpack_script_leaves_unknown_indexes_alone() {
        let decoded = unpack_script("1 99", 15, "a|b");
        assert_eq!(decoded, "b 99", "out-of-range references stay literal");
    }

    #[test]
    fn reverse_of_data_link_is_stable() {
        // The mirror expects the reversed blob; make the round trip explicit.
        let original = "7ec8e9edc09896";
        let reversed: String = original.chars().rev().collect();
        let back: String = reversed.chars().rev().collect();
        assert_eq!(back, original);
    }
}
