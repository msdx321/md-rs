use std::path::Path;
use std::sync::Arc;

use log::info;
use rustc_hash::FxHashMap as HashMap;
use tokio::sync::Mutex;

use crate::telegram::api::ApiState;
use crate::telegram::format::format_byte;

use super::paths::MediaPaths;
use super::progress::clear_progress;

const MAX_FILE_ID_CACHE: usize = 4096;

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn posix_fallocate(fd: i32, offset: i64, len: i64) -> i32;
}

/// Promote a completed `.part` file to its final name, record its file id in
/// the dedup cache, and notify the web UI. The caller has already validated
/// `actual` against the expected size.
pub(super) async fn finalize_download(
    msg_id: i32,
    fid: &str,
    file_ids: &Arc<Mutex<HashMap<String, u64>>>,
    paths: &MediaPaths,
    actual: u64,
    web_state: &Arc<ApiState>,
) -> anyhow::Result<()> {
    paths.validate_temp()?;
    paths.validate_final()?;
    let temp_path = &paths.temp;
    let final_path = &paths.final_path;
    tokio::fs::create_dir_all(final_path.parent().unwrap_or(Path::new("."))).await?;
    paths.validate_final()?;
    match tokio::fs::rename(temp_path, final_path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::CrossesDevices => {
            // Stage on the destination disk so a failed copy is never a completed file.
            let mut staging = final_path.as_os_str().to_owned();
            staging.push(".part");
            let staging = std::path::PathBuf::from(staging);
            tokio::fs::copy(temp_path, &staging).await?;
            tokio::fs::File::open(&staging).await?.sync_all().await?;
            tokio::fs::rename(&staging, final_path).await?;
            tokio::fs::remove_file(temp_path).await?;
        }
        Err(error) => return Err(error.into()),
    }
    clear_progress(temp_path, &web_state.database).await?;

    remember_file_id(msg_id, fid, file_ids, web_state).await;

    info!(
        "msg={msg_id}: saved {} -> {}",
        format_byte(actual as f64),
        final_path.display()
    );
    web_state.download_finished(msg_id, actual, true).await;
    Ok(())
}

/// Record a completed file ID in the bounded dedup cache and its database copy.
pub(super) async fn remember_file_id(
    msg_id: i32,
    fid: &str,
    file_ids: &Arc<Mutex<HashMap<String, u64>>>,
    web_state: &Arc<ApiState>,
) {
    if fid.is_empty() {
        return;
    }
    let now = crate::telegram::api::now_millis();
    let evicted = {
        let mut cache = file_ids.lock().await;
        // HashMap order is arbitrary, so this evicts a random entry (not truly
        // the oldest). Fine for a dedup cache: the worst case is a one-off
        // re-download of the evicted file.
        let evicted = (cache.len() >= MAX_FILE_ID_CACHE && !cache.contains_key(fid))
            .then(|| cache.keys().next().cloned())
            .flatten();
        if let Some(evicted) = &evicted {
            cache.remove(evicted);
        }
        cache.insert(fid.to_string(), now);
        evicted
    };
    if let Err(error) =
        crate::telegram::storage::record_file_id(&web_state.database, fid, now, evicted.as_deref())
            .await
    {
        log::warn!("msg={msg_id}: cannot save file id: {error:#}");
    }
}

/// Drop a partial `.part` download and its sidecar so the next attempt starts
/// fresh.
pub(super) async fn discard_partial(paths: &MediaPaths, database: &crate::storage::Database) {
    if let Err(error) = paths.validate_temp() {
        log::warn!("refusing unsafe partial cleanup: {error}");
        return;
    }
    let temp_path = &paths.temp;
    let _ = tokio::fs::remove_file(temp_path).await;
    if let Err(error) = clear_progress(temp_path, database).await {
        log::warn!("cannot clear resume checkpoint: {error}");
    }
}

/// Preallocate `size` bytes for `file`, preferring real block allocation
/// (`posix_fallocate`) so network-attached storage doesn't grow the file
/// incrementally during chunked writes. Falls back to a sparse `set_len` when
/// preallocation is unavailable (or on non-Linux hosts). No-op for `size == 0`.
pub(super) async fn preallocate(file: &tokio::fs::File, size: u64) -> anyhow::Result<()> {
    if size == 0 {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        let len = i64::try_from(size)?;
        // posix_fallocate is a blocking syscall; run it off the async thread.
        let rc =
            tokio::task::spawn_blocking(move || unsafe { posix_fallocate(fd, 0, len) }).await?;
        if rc != 0 {
            // ENOTSUP (e.g. over NFS) / EDQUOT / etc.: fall back to a sparse truncate.
            file.set_len(size).await?;
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        // No portable fallocate here; reserve the size sparsely instead.
        file.set_len(size).await?;
        Ok(())
    }
}
