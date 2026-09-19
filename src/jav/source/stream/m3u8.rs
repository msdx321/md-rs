//! HLS playlist parsing.
//!
//! Handles master playlists (variant selection), media playlists, fMP4 init
//! segments, byte ranges and AES-128 key declarations. Segments are not
//! assumed to be MPEG-TS: they may be `.m4s`, ADTS AAC, or extension-less
//! URLs with query strings.

use std::error::Error;

use regex::Regex;
use url::Url;

use crate::jav::util::{iv_for_segment, parse_hex};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Encryption {
    pub key_url: String,
    pub iv: [u8; 16],
}

/// One media segment (or init segment) of an HLS playlist.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Segment {
    pub url: String,
    /// `(start, length)` from `#EXT-X-BYTERANGE:length@start`.
    pub byte_range: Option<(u64, u64)>,
    pub encryption: Option<Encryption>,
}

#[derive(Debug, Clone)]
pub struct M3u8Info {
    /// Selected media playlist and its advertised quality, when available.
    pub variant: Variant,
    pub segments: Vec<Segment>,
    /// fMP4 init segment from `#EXT-X-MAP`, prepended when merging.
    pub init_segment: Option<Segment>,
    /// The selected external audio rendition, if audio is not muxed in video.
    pub audio: Option<Box<M3u8Info>>,
    pub total_duration: f64,
}

#[derive(Debug, Clone)]
pub struct Variant {
    pub uri: String,
    pub bandwidth: Option<usize>,
    pub resolution: Option<(usize, usize)>,
    pub audio_group: Option<String>,
}

fn parse_extinf_duration(line: &str) -> Option<f64> {
    let rest = line.strip_prefix("#EXTINF:")?;
    rest.split(',')
        .next()?
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|&d| d > 0.0)
}

/// Parse an unquoted `BYTERANGE` value (`82112@752`, or `82112` when the
/// range continues from the previous one).
fn parse_byterange_attr(value: &str, last_end: Option<u64>) -> Result<(u64, u64), &'static str> {
    let value = value.trim();
    let mut parts = value.split('@');
    let decimal = |value: &str| {
        let value = value.trim();
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err("invalid HLS byte range");
        }
        value.parse::<u64>().map_err(|_| "invalid HLS byte range")
    };
    let len = decimal(parts.next().ok_or("invalid HLS byte range")?)?;
    let start = match parts.next() {
        Some(value) => decimal(value)?,
        None => last_end.unwrap_or(0),
    };
    if parts.next().is_some() {
        return Err("invalid HLS byte range");
    }
    byte_range_end(start, len)?;
    Ok((start, len))
}

/// Exclusive end, shared by playlist continuation and HTTP range validation.
pub(crate) fn byte_range_end(start: u64, length: u64) -> Result<u64, &'static str> {
    start
        .checked_add(length)
        .filter(|_| length > 0)
        .ok_or("invalid HLS byte range")
}

/// Extract a quoted attribute such as `URI="init.mp4"`.
fn extract_attr(attrs: &str, name: &str) -> Option<String> {
    let re = Regex::new(&format!(
        r#"(?:^|,)\s*{}=\"([^\"]+)\""#,
        regex::escape(name)
    ))
    .ok()?;
    re.captures(attrs)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
}

/// Extract an unquoted attribute such as `METHOD=AES-128`.
fn extract_unquoted_attr(attrs: &str, name: &str) -> Option<String> {
    let re = Regex::new(&format!(r#"(?:^|,)\s*{}=([^,\s]+)"#, regex::escape(name))).ok()?;
    re.captures(attrs)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
}

pub fn parse_media_m3u8(text: &str, base_url: &Url) -> Result<M3u8Info, Box<dyn Error>> {
    let mut segments = Vec::new();
    let mut init_segment = None;
    let mut key_url = None;
    let mut iv = None;
    let mut sequence = 0usize;
    let mut total_duration = 0.0f64;
    let mut current_duration: Option<f64> = None;
    let mut pending_byterange: Option<(u64, u64)> = None;
    let mut last_byterange_end: Option<u64> = None;

    for raw_line in text.lines() {
        let line = raw_line.trim().trim_start_matches('\u{feff}');
        if line.is_empty() {
            continue;
        }

        if let Some(rest) = line.strip_prefix('#') {
            let _ = rest;
            if line.starts_with("#EXTINF:") {
                current_duration = parse_extinf_duration(line);
            } else if let Some(value) = line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
                sequence = value.trim().parse()?;
            } else if line.starts_with("#EXT-X-KEY:") {
                let attrs = line.strip_prefix("#EXT-X-KEY:").unwrap_or_default();
                match extract_unquoted_attr(attrs, "METHOD").as_deref() {
                    Some("NONE") => {
                        key_url = None;
                        iv = None;
                    }
                    Some("AES-128") => {
                        if extract_attr(attrs, "KEYFORMAT").is_some_and(|v| v != "identity") {
                            return Err("unsupported HLS key format".into());
                        }
                        let uri = extract_attr(attrs, "URI").ok_or("AES key URI is missing")?;
                        key_url = Some(base_url.join(&uri)?.to_string());
                        iv = extract_unquoted_attr(attrs, "IV")
                            .map(|value| {
                                parse_hex(&value)
                                    .filter(|bytes| !bytes.is_empty() && bytes.len() <= 16)
                                    .ok_or("invalid AES IV")
                            })
                            .transpose()?;
                    }
                    _ => return Err("unsupported HLS encryption method".into()),
                }
            } else if line.starts_with("#EXT-X-MAP") {
                let attrs = line.strip_prefix("#EXT-X-MAP:").unwrap_or_default().trim();
                if let Some(uri) = extract_attr(attrs, "URI") {
                    let resolved = base_url.join(&uri)?;
                    let mut range = None;
                    let mut quoted = false;
                    for attr in attrs.split(|c| {
                        if c == '"' {
                            quoted = !quoted;
                        }
                        c == ',' && !quoted
                    }) {
                        let (name, value) = attr.split_once('=').unwrap_or((attr, ""));
                        if name.trim() == "BYTERANGE" {
                            if range.is_some() {
                                return Err("duplicate HLS byte range".into());
                            }
                            let value = value
                                .trim()
                                .strip_prefix('"')
                                .and_then(|v| v.strip_suffix('"'))
                                .ok_or("invalid HLS byte range")?;
                            range = Some(parse_byterange_attr(value, last_byterange_end)?);
                        }
                    }
                    if let Some((start, len)) = range {
                        last_byterange_end = Some(byte_range_end(start, len)?);
                    }
                    if key_url.is_some() && iv.is_none() {
                        return Err("encrypted HLS init segment requires an explicit IV".into());
                    }
                    let init = Segment {
                        url: resolved.to_string(),
                        byte_range: range,
                        encryption: key_url.as_ref().map(|key_url| Encryption {
                            key_url: key_url.clone(),
                            iv: iv_for_segment(0, &iv),
                        }),
                    };
                    if init_segment
                        .as_ref()
                        .is_some_and(|previous| previous != &init)
                    {
                        return Err("changing HLS init segments are not supported".into());
                    }
                    init_segment = Some(init);
                }
            } else if line.starts_with("#EXT-X-BYTERANGE") {
                let value = line.strip_prefix("#EXT-X-BYTERANGE:").unwrap_or_default();
                let range = parse_byterange_attr(value, last_byterange_end)?;
                last_byterange_end = Some(byte_range_end(range.0, range.1)?);
                pending_byterange = Some(range);
            }
        } else {
            segments.push(Segment {
                url: base_url.join(line)?.to_string(),
                byte_range: pending_byterange.take(),
                encryption: key_url.as_ref().map(|key_url| Encryption {
                    key_url: key_url.clone(),
                    iv: iv_for_segment(sequence, &iv),
                }),
            });
            sequence = sequence
                .checked_add(1)
                .ok_or("HLS media sequence overflow")?;
            if let Some(dur) = current_duration.take() {
                total_duration += dur;
            }
        }
    }

    if total_duration == 0.0 && !segments.is_empty() {
        // Playlists without EXTINF: assume the usual 6s target duration.
        total_duration = segments.len() as f64 * 6.0;
    }

    Ok(M3u8Info {
        variant: Variant {
            uri: base_url.to_string(),
            bandwidth: None,
            resolution: None,
            audio_group: None,
        },
        segments,
        init_segment,
        audio: None,
        total_duration,
    })
}

fn parse_stream_inf(line: &str) -> (Option<usize>, Option<(usize, usize)>) {
    let mut bandwidth = None;
    let mut resolution = None;
    let parts = line.strip_prefix("#EXT-X-STREAM-INF:").unwrap_or("");
    for attr in parts.split(',') {
        let Some((key, val)) = attr.split_once('=') else {
            continue;
        };
        match key.trim().to_uppercase().as_str() {
            "BANDWIDTH" => bandwidth = val.trim().parse::<usize>().ok(),
            "RESOLUTION" => {
                if let Some((w, h)) = val.trim().split_once('x')
                    && let (Ok(w), Ok(h)) = (w.parse::<usize>(), h.parse::<usize>())
                {
                    resolution = Some((w, h));
                }
            }
            _ => {}
        }
    }
    (bandwidth, resolution)
}

/// Pick a variant by preference: `lowest`, `highest`, or a target height.
pub fn select_variant(variants: &[Variant], pref: &str) -> Option<Variant> {
    if variants.is_empty() {
        return None;
    }
    let pref = pref.trim().to_lowercase();
    let mut sorted = variants.to_vec();
    sorted.sort_by_key(|v| {
        (
            v.resolution.map(|r| r.1).unwrap_or(0),
            v.bandwidth.unwrap_or(0),
        )
    });

    match pref.as_str() {
        "lowest" => return sorted.first().cloned(),
        "highest" | "" => return sorted.last().cloned(),
        _ => {}
    }

    if let Ok(target) = pref.parse::<usize>() {
        let at_or_below: Vec<Variant> = sorted
            .iter()
            .filter(|v| v.resolution.map(|r| r.1).unwrap_or(0) <= target)
            .cloned()
            .collect();
        if !at_or_below.is_empty() {
            return at_or_below.last().cloned();
        }
    }
    sorted.last().cloned()
}

/// Split a master playlist into its variants, resolving relative URIs.
pub fn parse_master_variants(text: &str, base_url: &Url) -> Vec<Variant> {
    let mut variants = Vec::new();
    let lines: Vec<&str> = text.lines().map(str::trim).collect();
    let mut i = 0;
    while i < lines.len() {
        if lines[i].starts_with("#EXT-X-STREAM-INF:") {
            let (bandwidth, resolution) = parse_stream_inf(lines[i]);
            let mut j = i + 1;
            while j < lines.len() {
                let next = lines[j];
                if !next.is_empty() && !next.starts_with('#') {
                    if let Ok(uri) = base_url.join(next) {
                        variants.push(Variant {
                            uri: uri.to_string(),
                            bandwidth,
                            resolution,
                            audio_group: extract_attr(
                                lines[i]
                                    .strip_prefix("#EXT-X-STREAM-INF:")
                                    .unwrap_or_default(),
                                "AUDIO",
                            ),
                        });
                    }
                    break;
                }
                j += 1;
            }
            i = j;
        }
        i += 1;
    }
    variants
}

/// Prefer the group's default rendition, then autoselect, then playlist order.
/// A rendition without a URI declares audio carried in the video playlist.
pub fn select_audio_uri(
    text: &str,
    base_url: &Url,
    group: &str,
) -> Result<Option<String>, Box<dyn Error>> {
    let mut renditions: Vec<_> = text
        .lines()
        .filter_map(|line| line.trim().strip_prefix("#EXT-X-MEDIA:"))
        .filter(|attrs| {
            extract_unquoted_attr(attrs, "TYPE").as_deref() == Some("AUDIO")
                && extract_attr(attrs, "GROUP-ID").as_deref() == Some(group)
        })
        .collect();
    renditions.sort_by_key(|attrs| {
        (
            extract_unquoted_attr(attrs, "DEFAULT").as_deref() != Some("YES"),
            extract_unquoted_attr(attrs, "AUTOSELECT").as_deref() != Some("YES"),
        )
    });
    let attrs = renditions
        .first()
        .ok_or("HLS audio group has no renditions")?;
    extract_attr(attrs, "URI")
        .map(|uri| base_url.join(&uri).map(String::from).map_err(Into::into))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Url {
        Url::parse("https://cdn.example.com/video/index.m3u8").unwrap()
    }

    #[test]
    fn rejects_zero_overflow_and_malformed_byte_ranges() {
        for value in [
            "0",
            "0@1",
            "1@18446744073709551615",
            "2@18446744073709551614",
            "18446744073709551616@0",
            "1@18446744073709551616",
            "1@",
            "1@2@3",
            "-1@0",
            "+1@0",
            "1@-1",
            "abc",
            "",
            "\"1@0\"",
        ] {
            let text = format!("#EXTM3U\n#EXT-X-BYTERANGE:{value}\na.ts\n");
            assert!(
                parse_media_m3u8(&text, &base()).is_err(),
                "accepted segment {value:?}"
            );
            let text = format!("#EXTM3U\n#EXT-X-MAP:URI=\"a.mp4\",BYTERANGE=\"{value}\"\na.ts\n");
            assert!(
                parse_media_m3u8(&text, &base()).is_err(),
                "accepted map {value:?}"
            );
        }
        for attr in [
            "BYTERANGE",
            "BYTERANGE=",
            "BYTERANGE=1@0",
            "BYTERANGE=\"1@0",
            "BYTERANGE=\"1@0\"junk",
            "BYTERANGE=\"1@0\",BYTERANGE=\"2@0\"",
        ] {
            let text = format!("#EXTM3U\n#EXT-X-MAP:URI=\"a.mp4\",{attr}\na.ts\n");
            assert!(parse_media_m3u8(&text, &base()).is_err(), "accepted {attr}");
        }
        let text =
            "#EXTM3U\n#EXT-X-BYTERANGE:18446744073709551615@0\na.ts\n#EXT-X-BYTERANGE:1\na.ts\n";
        assert!(
            parse_media_m3u8(text, &base()).is_err(),
            "implicit continuation overflow"
        );
    }

    #[test]
    fn valid_byte_range_boundaries_keep_cache_encoding_and_continuation() {
        let text = "#EXTM3U\n#EXT-X-MAP:URI=\"a.mp4\",BYTERANGE=\"4@2\"\n#EXT-X-BYTERANGE:3\na.mp4\n#EXT-X-BYTERANGE:1@18446744073709551614\na.mp4\n";
        let info = parse_media_m3u8(text, &base()).unwrap();
        assert_eq!(info.init_segment.unwrap().byte_range, Some((2, 4)));
        assert_eq!(info.segments[0].byte_range, Some((6, 3)));
        assert_eq!(info.segments[1].byte_range, Some((u64::MAX - 1, 1)));
        assert_eq!(
            serde_json::to_string(&info.segments[0]).unwrap(),
            r#"{"url":"https://cdn.example.com/video/a.mp4","byte_range":[6,3],"encryption":null}"#
        );
        assert_eq!(
            parse_byterange_attr("18446744073709551615@0", None),
            Ok((0, u64::MAX))
        );
        assert_eq!(parse_byterange_attr("3", None), Ok((0, 3)));
        // A comma inside a quoted URI is not a new attribute.
        let info = parse_media_m3u8(
            "#EXTM3U\n#EXT-X-MAP:URI=\"a,BYTERANGE=invalid.mp4\",BYTERANGE=\"3@0\"\na.mp4\n",
            &base(),
        )
        .unwrap();
        assert_eq!(info.init_segment.unwrap().byte_range, Some((0, 3)));
    }

    #[test]
    fn parses_plain_ts_playlist() {
        let text = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n\
#EXTINF:10.0,\nseg0.ts\n#EXTINF:9.5,\nsub/seg1.ts\n";
        let info = parse_media_m3u8(text, &base()).unwrap();
        assert_eq!(info.segments.len(), 2);
        assert_eq!(
            info.segments[0].url,
            "https://cdn.example.com/video/seg0.ts"
        );
        assert_eq!(
            info.segments[1].url,
            "https://cdn.example.com/video/sub/seg1.ts"
        );
        assert_eq!(info.segments[0].byte_range, None);
        assert!(info.init_segment.is_none());
        assert!(info.segments[0].encryption.is_none());
        assert_eq!(info.total_duration, 19.5);
    }

    #[test]
    fn parses_aes_key_and_iv() {
        let text = "#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",IV=0x0000000000000000000000000000002a\n\
#EXTINF:10.0,\ns0.ts\n";
        let info = parse_media_m3u8(text, &base()).unwrap();
        assert_eq!(
            info.segments[0]
                .encryption
                .as_ref()
                .map(|e| e.key_url.as_str()),
            Some("https://cdn.example.com/video/key.bin")
        );
        let mut expected = [0u8; 16];
        expected[15] = 0x2a;
        assert_eq!(info.segments[0].encryption.as_ref().unwrap().iv, expected);
    }

    #[test]
    fn parses_fmp4_with_map_and_byterange() {
        let text = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:6\n\
#EXT-X-MAP:URI=\"init.mp4\",BYTERANGE=\"720@0\"\n\
#EXTINF:6.0,\nseg1.m4s\n\
#EXT-X-BYTERANGE:82112@752\n#EXTINF:6.0,\nseg2.m4s\n\
#EXTINF:6.0,\n/other/seg3.m4s\n\
#EXTINF:6.0,\n//other.example.com/x/seg4.m4s\n";
        let info = parse_media_m3u8(text, &base()).unwrap();
        let init = info.init_segment.expect("init segment parsed");
        assert_eq!(init.url, "https://cdn.example.com/video/init.mp4");
        assert_eq!(init.byte_range, Some((0, 720)));
        assert_eq!(info.segments.len(), 4);
        assert_eq!(info.segments[1].byte_range, Some((752, 82112)));
        assert_eq!(
            info.segments[2].url,
            "https://cdn.example.com/other/seg3.m4s"
        );
        assert_eq!(info.segments[3].url, "https://other.example.com/x/seg4.m4s");
    }

    #[test]
    fn byterange_without_offset_continues_from_previous() {
        let text = "#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\",BYTERANGE=\"720@0\"\n\
#EXTINF:6.0,\nseg1.m4s\n\
#EXT-X-BYTERANGE:82112\n#EXTINF:6.0,\nseg2.m4s\n";
        let info = parse_media_m3u8(text, &base()).unwrap();
        assert_eq!(info.init_segment.unwrap().byte_range, Some((0, 720)));
        assert_eq!(info.segments[1].byte_range, Some((720, 82112)));
    }

    #[test]
    fn key_method_none_disables_encryption() {
        let text = "#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n\
#EXTINF:10.0,\ns0.ts\n\
#EXT-X-KEY:METHOD=NONE\n\
#EXTINF:10.0,\ns1.ts\n";
        let info = parse_media_m3u8(text, &base()).unwrap();
        assert!(
            info.segments[1].encryption.is_none(),
            "later METHOD=NONE must clear the key for subsequent segments"
        );
        assert_eq!(info.segments.len(), 2);
    }

    #[test]
    fn segments_with_query_strings_and_no_extension() {
        let text = "#EXTM3U\n#EXTINF:6.0,\nseg1?token=abc&exp=1\n#EXTINF:6.0,\nclip\n";
        let info = parse_media_m3u8(text, &base()).unwrap();
        assert_eq!(
            info.segments[0].url,
            "https://cdn.example.com/video/seg1?token=abc&exp=1"
        );
        assert_eq!(info.segments[1].url, "https://cdn.example.com/video/clip");
    }

    #[test]
    fn key_attributes_in_any_order_and_bom() {
        let text = "\u{feff}#EXTM3U\n#EXT-X-KEY:URI=\"key.bin\",METHOD=AES-128,KEYFORMAT=\"identity\"\n\
#EXTINF:10.0,\ns0.ts\n";
        let info = parse_media_m3u8(text, &base()).unwrap();
        assert_eq!(
            info.segments[0]
                .encryption
                .as_ref()
                .map(|e| e.key_url.as_str()),
            Some("https://cdn.example.com/video/key.bin")
        );
        assert_eq!(info.segments.len(), 1, "BOM line must not become a segment");
    }

    #[test]
    fn parses_master_playlist_variants() {
        let text = "#EXTM3U\n\
#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360\nlow/index.m3u8\n\
#EXT-X-STREAM-INF:BANDWIDTH=2400000,RESOLUTION=1280x720\nhigh/index.m3u8\n";
        let variants = parse_master_variants(text, &base());
        assert_eq!(variants.len(), 2);
        assert_eq!(
            variants[1].uri,
            "https://cdn.example.com/video/high/index.m3u8"
        );
        assert_eq!(variants[1].resolution, Some((1280, 720)));
    }

    #[test]
    fn variant_selection_modes() {
        let variants = vec![
            Variant {
                uri: "a".into(),
                audio_group: None,
                bandwidth: Some(1),
                resolution: Some((640, 360)),
            },
            Variant {
                uri: "b".into(),
                audio_group: None,
                bandwidth: Some(2),
                resolution: Some((1280, 720)),
            },
            Variant {
                uri: "c".into(),
                audio_group: None,
                bandwidth: Some(3),
                resolution: Some((1920, 1080)),
            },
        ];
        assert_eq!(select_variant(&variants, "highest").unwrap().uri, "c");
        assert_eq!(select_variant(&variants, "lowest").unwrap().uri, "a");
        assert_eq!(select_variant(&variants, "720").unwrap().uri, "b");
        assert_eq!(select_variant(&variants, "480").unwrap().uri, "a");
        assert_eq!(select_variant(&variants, "2160").unwrap().uri, "c");
    }
}
