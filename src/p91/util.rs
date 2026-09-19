//! Common video filename and speed policies, reexported for 91Porn callers.

pub use crate::runtime::{download_speed::SpeedTracker, video_filename::sanitize_filename};

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

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
