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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Database;

    #[tokio::test]
    async fn resume_offset_clamps_disk_checkpoint_and_total_and_preserves_complete_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("media.part");
        let db = Database::open(":memory:").await.unwrap();
        let c = DOWNLOAD_CHUNK_SIZE;
        for (len, checkpoint, total, expected) in [
            (3 * c, 0, 3 * c, 0), // Preallocation alone proves nothing.
            (3 * c, c + 17, 3 * c, c),
            (c + 17, 2 * c, 3 * c, c),
            (3 * c, 4 * c, 3 * c, 0),
            (4 * c, c, 3 * c, 0),
            (c + 17, c + 17, c + 17, c + 17), // Complete unaligned tail.
            (c, c, 0, 0),                     // Unknown-size transfers restart.
        ] {
            let file = tokio::fs::File::create(&path).await.unwrap();
            file.set_len(len).await.unwrap();
            write_progress(&path, checkpoint, &db).await.unwrap();
            assert_eq!(
                resume_offset(&path, total, &db).await.unwrap(),
                expected,
                "len={len} checkpoint={checkpoint} total={total}"
            );
        }
        tokio::fs::remove_file(&path).await.unwrap();
        assert_eq!(resume_offset(&path, 3 * c, &db).await.unwrap(), 0);
        assert_eq!(
            crate::migration::resume_offset(&path, &db).await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn legacy_checkpoint_import_survives_reopen_and_zero_prevents_resurrection() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let path = dir.path().join("legacy.part");
        let sidecar = dir.path().join("legacy.part.progress");
        let total = 3 * DOWNLOAD_CHUNK_SIZE;
        tokio::fs::File::create(&path)
            .await
            .unwrap()
            .set_len(total)
            .await
            .unwrap();
        tokio::fs::write(&sidecar, format!(" {}\n", DOWNLOAD_CHUNK_SIZE + 13))
            .await
            .unwrap();
        let db = Database::open(&db_path).await.unwrap();
        assert_eq!(
            resume_offset(&path, total, &db).await.unwrap(),
            DOWNLOAD_CHUNK_SIZE
        );
        drop(db);
        tokio::fs::write(&sidecar, total.to_string()).await.unwrap();
        let db = Database::open(&db_path).await.unwrap();
        assert_eq!(
            resume_offset(&path, total, &db).await.unwrap(),
            DOWNLOAD_CHUNK_SIZE
        );
        clear_progress(&path, &db).await.unwrap();
        drop(db);
        let db = Database::open(&db_path).await.unwrap();
        assert_eq!(resume_offset(&path, total, &db).await.unwrap(), 0);
        assert!(
            sidecar.exists(),
            "compatibility hook must not delete legacy data"
        );
    }
}
