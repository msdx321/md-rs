//! Shared video-only resolution policy. Missing or zero dimensions are unknown.

pub fn rejection(minimum: u32, dimensions: Option<(u64, u64)>) -> Option<String> {
    let (width, height) = dimensions?;
    (minimum > 0 && width > 0 && height > 0 && width.min(height) < u64::from(minimum))
        .then(|| format!("Skipped: video is {width}×{height}, below the {minimum}p minimum"))
}
