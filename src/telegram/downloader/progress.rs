use std::path::Path;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressStyle};

use crate::telegram::api::ApiState;
pub(super) use crate::telegram::storage::checkpoints::{clear_progress, write_progress};

pub(super) const DOWNLOAD_CHUNK_SIZE: u64 = 512 * 1024;
pub(super) const PROGRESS_REPORT_INTERVAL: Duration = Duration::from_millis(2000);

static DOWNLOAD_PROGRESS_STYLE: LazyLock<ProgressStyle> = LazyLock::new(|| {
    ProgressStyle::with_template(
        "{msg:>8} {wide_bar:.cyan/blue} {bytes:>8}/{total_bytes:8} {bytes_per_sec:>10} {eta}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("##-")
});

pub(super) struct DownloadProgress<'a> {
    pub(super) pb: &'a ProgressBar,
    pub(super) web_state: &'a Arc<ApiState>,
    pub(super) msg_id: i32,
}

pub(super) fn progress_style() -> ProgressStyle {
    DOWNLOAD_PROGRESS_STYLE.clone()
}

pub(super) async fn report_download_progress(
    progress: &DownloadProgress<'_>,
    start: u64,
    downloaded: u64,
    started: Instant,
) {
    progress.pb.set_position(downloaded);
    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    progress
        .web_state
        .download_progress(
            progress.msg_id,
            downloaded,
            ((downloaded - start) as f64 / elapsed) as u64,
        )
        .await;
}

/// Resume from the last durable byte checkpoint, never from preallocated size.
/// Legacy `.progress` sidecars are imported lazily when their download resumes.
pub(super) async fn resume_offset(
    temp_path: &Path,
    total: u64,
    database: &crate::storage::Database,
) -> anyhow::Result<u64> {
    let len = match tokio::fs::metadata(temp_path).await {
        Ok(m) => m.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            write_progress(temp_path, 0, database).await?;
            return Ok(0);
        }
        Err(e) => return Err(e.into()),
    };
    let valid = crate::migration::resume_offset(temp_path, database).await?;
    if valid > total || len > total {
        return Ok(0);
    }
    if total > 0 && valid == total && len == total {
        return Ok(total);
    }
    let valid = valid.min(len).min(total);
    Ok(valid - (valid % DOWNLOAD_CHUNK_SIZE))
}
