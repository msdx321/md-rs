//! Small shared helpers ported from the original Tauri implementation:
//! filename sanitising, AES key/IV derivation, the
//! fake-header stripper some hosts prepend to segments, and a sliding-window
//! speed estimator for the UI.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Replace every character that is unsafe in a filename.
pub fn sanitize_filename(title: &str) -> String {
    let cleaned: String = title
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => '_',
            c if (c as u32) < 0x20 => '_',
            c => c,
        })
        .collect();
    let cleaned = cleaned.trim().trim_matches('.').to_string();
    // Keep well under the 255-byte limit most filesystems impose, counting
    // UTF-8 bytes rather than characters.
    truncate_bytes(&cleaned, 180)
}

fn truncate_bytes(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].trim_end().to_string()
}

/// Parse a `0x…` hex blob into bytes.
pub fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    if !s.len().is_multiple_of(2) || s.is_empty() {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// CBC IV for segment `index`: the playlist's explicit IV, or the segment
/// number as a big-endian 128-bit integer (the HLS default).
pub fn iv_for_segment(index: usize, custom_iv: &Option<Vec<u8>>) -> [u8; 16] {
    if let Some(iv) = custom_iv {
        let mut out = [0u8; 16];
        let len = iv.len().min(16);
        out[16 - len..].copy_from_slice(&iv[..len]);
        out
    } else {
        (index as u128).to_be_bytes()
    }
}

/// Some hosts prepend a fake image header to each segment to defeat naive
/// downloaders. Find the first MPEG-TS sync byte that starts a run of five
/// evenly spaced packets and return the payload from there.
pub fn strip_fake_header(data: &[u8]) -> &[u8] {
    if data.is_empty() {
        return data;
    }
    if data[0] == 0x47 {
        return data;
    }
    if data.len() < 188 * 4 + 1 {
        return &[];
    }
    let limit = (data.len() - 188 * 4 - 1).min(8000);
    let mut i = 0;
    while i <= limit {
        match data[i..=limit].iter().position(|&b| b == 0x47) {
            Some(pos) => {
                let j = i + pos;
                let aligned = (0..5).all(|n| data.get(j + 188 * n) == Some(&0x47));
                if aligned {
                    return &data[j..];
                }
                i = j + 1;
            }
            None => break,
        }
    }
    &[]
}

/// Append an actionable hint when an error looks like a Cloudflare block.
pub fn cloudflare_hint(err: &str) -> String {
    let lower = err.to_ascii_lowercase();
    if lower.contains("media request") {
        return err.to_string();
    }
    let looks_blocked = crate::source::cf::is_challenge_error(err) || lower.contains("forbidden");
    if looks_blocked {
        format!(
            "{err} (Cloudflare rejected the request — a fresh cf_clearance cookie may be needed)"
        )
    } else {
        err.to_string()
    }
}

/// Sliding-window download speed estimator.
///
/// Speed is the byte delta across the whole window divided by the window's
/// elapsed time, which smooths the bursty arrival of network chunks instead of
/// flipping between 0 and a spike whenever a chunk lands.
pub struct SpeedTracker {
    samples: VecDeque<(Instant, u64)>,
    window: Duration,
}

impl SpeedTracker {
    pub fn new(window: Duration) -> Self {
        Self {
            samples: VecDeque::new(),
            window,
        }
    }

    pub fn add_sample(&mut self, now: Instant, bytes: u64) {
        self.samples.push_back((now, bytes));
        while let Some((t, _)) = self.samples.front() {
            if now.duration_since(*t) > self.window {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Average speed in KB/s over the window; 0.0 until there is enough data.
    pub fn speed(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }
        let (start_t, start_b) = self.samples.front().unwrap();
        let (end_t, end_b) = self.samples.back().unwrap();
        let dt = end_t.duration_since(*start_t).as_secs_f64();
        if dt <= 0.0 {
            return 0.0;
        }
        end_b.saturating_sub(*start_b) as f64 / 1024.0 / dt
    }

    pub fn sample(&mut self, now: Instant, bytes: u64) -> f64 {
        self.add_sample(now, bytes);
        self.speed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_replaces_reserved_characters() {
        assert_eq!(
            sanitize_filename("a/b:c*d?e\"f<g>h|i\\j"),
            "a_b_c_d_e_f_g_h_i_j"
        );
        assert_eq!(sanitize_filename("  ..name..  "), "name");
    }

    #[test]
    fn sanitize_truncates_on_char_boundary() {
        let long = "あ".repeat(200);
        let out = sanitize_filename(&long);
        assert!(out.len() <= 180);
        assert!(out.chars().all(|c| c == 'あ'));
    }

    #[test]
    fn hex_parsing() {
        assert_eq!(parse_hex("0x00ff"), Some(vec![0, 255]));
        assert_eq!(parse_hex("abc"), None);
    }

    #[test]
    fn iv_defaults_to_segment_index() {
        let iv = iv_for_segment(2, &None);
        assert_eq!(iv[15], 2);
        assert_eq!(iv_for_segment(0, &Some(vec![0x2a]))[15], 0x2a);
    }

    #[test]
    fn strips_leading_fake_header() {
        let mut data = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
        data.extend(std::iter::repeat_n(0x00, 40));
        let start = data.len();
        for _ in 0..5 {
            let mut packet = vec![0x47];
            packet.extend(std::iter::repeat_n(0x11, 187));
            data.extend(packet);
        }
        let stripped = strip_fake_header(&data);
        assert_eq!(stripped.len(), data.len() - start);
        assert_eq!(stripped[0], 0x47);
    }

    #[test]
    fn keeps_data_that_already_starts_with_sync_byte() {
        let data = vec![0x47; 188 * 5];
        assert_eq!(strip_fake_header(&data).len(), data.len());
    }

    #[test]
    fn speed_tracker_averages_over_window() {
        let mut st = SpeedTracker::new(Duration::from_secs(3));
        let t0 = Instant::now();
        let mut speed = 0.0;
        for i in 1..=4u64 {
            speed = st.sample(t0 + Duration::from_millis(i * 250), i * 256 * 1024);
        }
        assert!((speed - 1024.0).abs() < 1.0, "got {speed}");
    }

    #[test]
    fn speed_tracker_drops_old_samples() {
        let mut st = SpeedTracker::new(Duration::from_secs(3));
        let t0 = Instant::now();
        st.add_sample(t0, 0);
        st.add_sample(t0 + Duration::from_secs(1), 1024 * 1024);
        st.add_sample(t0 + Duration::from_secs(4), 1024 * 1024);
        assert_eq!(st.speed(), 0.0);
        st.add_sample(t0 + Duration::from_secs(5), 2 * 1024 * 1024);
        assert!((st.speed() - 1024.0).abs() < 1.0);
    }
}
