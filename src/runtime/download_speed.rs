//! Sliding-window download speed reporting, independent of transfer protocol.
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Byte delta across the whole window divided by elapsed time, smoothing the
/// bursty arrival of chunks instead of reporting a spike for each chunk.
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
    fn speed_tracker_preserves_window_boundary_and_zero_delta_rules() {
        let start = Instant::now();
        let mut tracker = SpeedTracker::new(Duration::from_secs(3));
        assert_eq!(tracker.speed(), 0.0);
        assert_eq!(tracker.sample(start, 100), 0.0);
        assert_eq!(tracker.sample(start, 200), 0.0);
        assert_eq!(tracker.sample(start + Duration::from_secs(3), 3172), 1.0);
        assert_eq!(tracker.sample(start + Duration::from_secs(4), 0), 0.0);
        assert_eq!(tracker.sample(start + Duration::from_secs(8), 1024), 0.0);
    }
}
