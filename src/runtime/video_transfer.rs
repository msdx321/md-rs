//! Final size checks for progressive MP4 transfers.

pub fn validate_size(written: u64, expected: Option<u64>) -> anyhow::Result<()> {
    anyhow::ensure!(written > 0, "empty media response");
    if let Some(total) = expected {
        anyhow::ensure!(
            written == total,
            "incomplete download: {written}/{total} bytes"
        );
    }
    Ok(())
}
