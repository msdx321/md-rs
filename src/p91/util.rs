//! Small shared helpers for the 91Porn module: filename sanitising and a
//! sliding-window speed estimator for the UI.

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

    pub fn sample(&mut self, now: Instant, bytes: u64) -> f64 {
        self.samples.push_back((now, bytes));
        while let Some((t, _)) = self.samples.front() {
            if now.duration_since(*t) > self.window {
                self.samples.pop_front();
            } else {
                break;
            }
        }
        if self.samples.len() < 2 {
            return 0.0;
        }
        let (start_t, start_b) = self.samples.front().expect("non-empty");
        let (end_t, end_b) = self.samples.back().expect("non-empty");
        let dt = end_t.duration_since(*start_t).as_secs_f64();
        if dt <= 0.0 {
            return 0.0;
        }
        end_b.saturating_sub(*start_b) as f64 / 1024.0 / dt
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
    fn speed_tracker_averages_over_window() {
        let mut st = SpeedTracker::new(Duration::from_secs(3));
        let t0 = Instant::now();
        let mut speed = 0.0;
        for i in 1..=4u64 {
            speed = st.sample(t0 + Duration::from_millis(i * 250), i * 256 * 1024);
        }
        assert!((speed - 1024.0).abs() < 1.0, "got {speed}");
    }
}
