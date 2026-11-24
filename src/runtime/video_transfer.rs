//! Buffered writes and size checks for progressive MP4 transfers.
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;

use super::video_tasks::{TaskState, VideoTasks};

const WRITE_BUFFER: usize = 1024 * 1024;

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

/// Write the response body into `part`. Returns `true` when the body finished,
/// `false` when the task was paused or cancelled mid-stream.
pub(crate) async fn write_body<F: Future<Output = ()>>(
    tasks: &VideoTasks,
    id: &str,
    response: wreq::Response,
    part: &Path,
    append: bool,
    downloaded: &AtomicU64,
    acquire: impl Fn(usize) -> F,
) -> anyhow::Result<bool> {
    let running = || tasks.task_control(id) == Some(TaskState::Running);
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).write(true);
    if append {
        options.append(true);
    } else {
        options.truncate(true);
    }
    // Batch network bursts into fewer blocking filesystem writes.
    let mut file = tokio::io::BufWriter::with_capacity(WRITE_BUFFER, options.open(part).await?);

    // Tokio's file writer can still own a blocking write when write_all returns.
    // Drain it on network/write errors too, before the file lease can be released.
    let result = async {
        let mut stream = response.bytes_stream();
        // Only state transitions wake this, not every task's progress update.
        let mut changes = tasks.subscribe_status();
        loop {
            if !running() {
                return Ok(false);
            }
            // Buffer only bursts that are already available; while the network
            // is idle, write through so the file holds every received byte.
            let next = match futures_util::FutureExt::now_or_never(stream.next()) {
                Some(next) => Ok(next),
                None => {
                    file.flush().await?;
                    // A state change cancels the pending read. Nothing is lost:
                    // whatever was not written yet is simply re-requested from
                    // the new file length.
                    tokio::select! {
                        biased;
                        changed = changes.changed() => {
                            if changed.is_err() {
                                return Ok(false);
                            }
                            continue;
                        }
                        result = tokio::time::timeout(Duration::from_secs(60), stream.next()) => result,
                    }
                }
            };
            let chunk = match next {
                Err(_) => anyhow::bail!("media read timed out"),
                Ok(None) => break,
                Ok(Some(Err(error))) => return Err(error.into()),
                Ok(Some(Ok(chunk))) => chunk,
            };
            for piece in chunk.chunks(64 * 1024) {
                let acquire = acquire(piece.len());
                tokio::pin!(acquire);
                loop {
                    tokio::select! {
                        biased;
                        changed = changes.changed() => {
                            if changed.is_err() || !running() {
                                return Ok(false);
                            }
                        }
                        () = &mut acquire => break,
                    }
                }
                file.write_all(piece).await?;
                downloaded.fetch_add(piece.len() as u64, Ordering::Relaxed);
            }
        }
        Ok::<_, anyhow::Error>(true)
    }
    .await;
    let drained = flush(&mut file).await;
    let complete = result?;
    drained?;
    Ok(complete && running())
}

async fn flush(file: &mut tokio::io::BufWriter<tokio::fs::File>) -> std::io::Result<()> {
    file.flush().await?;
    file.get_mut().sync_data().await
}

/// Total body length, from `Content-Range` when resuming, else `Content-Length`.
pub(crate) fn content_total(response: &wreq::Response, offset: u64) -> Option<u64> {
    let headers = response.headers();
    if let Some(value) = headers.get("content-range").and_then(|v| v.to_str().ok())
        && let Some(total) = total_from_content_range(value)
    {
        return Some(total);
    }
    headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|value| total_from_content_length(value, offset))
}

/// `bytes 100-199/500` → `500`.
fn total_from_content_range(value: &str) -> Option<u64> {
    value.rsplit('/').next()?.trim().parse().ok()
}

fn total_from_content_length(value: &str, offset: u64) -> Option<u64> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .and_then(|length| length.checked_add(offset))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_total_reads_content_range_first() {
        assert_eq!(total_from_content_range("bytes 100-199/500"), Some(500));
        assert_eq!(total_from_content_range("not a range"), None);
    }

    #[test]
    fn content_total_falls_back_to_content_length() {
        assert_eq!(total_from_content_length("250", 0), Some(250));
        assert_eq!(total_from_content_length("150", 100), Some(250));
        assert_eq!(total_from_content_length("", 0), None);
    }

    #[test]
    fn content_totals_check_u64_boundaries_without_wrapping() {
        let max = u64::MAX.to_string();
        assert_eq!(total_from_content_length(&max, 0), Some(u64::MAX));
        assert_eq!(total_from_content_length(&max, 1), None);
        assert_eq!(total_from_content_length("1", u64::MAX - 1), Some(u64::MAX));
        assert_eq!(total_from_content_length("2", u64::MAX - 1), None);
        assert_eq!(total_from_content_length("0", u64::MAX), Some(u64::MAX));
        assert_eq!(total_from_content_length("18446744073709551616", 0), None);
        assert_eq!(total_from_content_length("invalid", 1), None);
        // Content-Range supplies an absolute total, not a length to add to
        // the resumed offset. Its checked integer parsing needs no arithmetic.
        assert_eq!(
            total_from_content_range(&format!("bytes 1-2/{max}")),
            Some(u64::MAX)
        );
        assert_eq!(
            total_from_content_range("bytes 1-2/18446744073709551616"),
            None
        );
        assert_eq!(total_from_content_range("bytes 1-2/*"), None);
    }
}
