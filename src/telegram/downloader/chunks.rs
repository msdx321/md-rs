use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use grammers_client::Client;
use grammers_client::media::Media;
use log::warn;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::runtime::download_limiter::DownloadModule;
use crate::telegram::api::ApiState;

use super::finalize::preallocate;
use super::progress::{
    DOWNLOAD_CHUNK_SIZE, DownloadProgress, PROGRESS_REPORT_INTERVAL, report_download_progress,
    write_progress,
};
use crate::telegram::app::{Shutdown, flood_wait_secs, sleep_cancellable, wait_paused};

const RETRY_DELAY_SECS: u64 = 5;
const CHUNK_RETRY_LIMIT: u32 = 3;
const PROGRESS_FLUSH_BYTES: u64 = 16 * 1024 * 1024;

async fn send_chunk(
    tx: &mpsc::Sender<Vec<u8>>,
    chunk: Vec<u8>,
    shutdown: &Shutdown,
) -> Result<(), mpsc::error::SendError<Vec<u8>>> {
    tokio::select! {
        permit = tx.reserve() => match permit {
            Ok(permit) => {
                permit.send(chunk);
                Ok(())
            }
            Err(_) => Err(mpsc::error::SendError(chunk)),
        },
        _ = shutdown.cancelled() => Err(mpsc::error::SendError(chunk)),
    }
}

pub(super) async fn download_concurrent(
    client: &Client,
    media: &Media,
    path: &Path,
    range: std::ops::Range<u64>,
    connections: u64,
    progress: &DownloadProgress<'_>,
    shutdown: &Shutdown,
) -> anyhow::Result<()> {
    let start = range.start;
    let total = range.end;
    let chunk_size = DOWNLOAD_CHUNK_SIZE;
    let start_chunk = start / chunk_size;
    let total_chunks = total.div_ceil(chunk_size);
    let workers = connections.min(total_chunks - start_chunk).max(1);

    // Each striped worker has a bounded queue, consumed in file order. A slow
    // chunk therefore cannot make faster workers buffer the rest of the file.
    let mut receivers = Vec::with_capacity(workers as usize);
    // Set by the first worker to fail so its peers stop fetching after their
    // current chunk instead of downloading data that will be discarded.
    let abort = Arc::new(AtomicBool::new(false));
    let mut tasks = JoinSet::new();

    for worker in 0..workers {
        let client = client.clone();
        let media = media.clone();
        let (tx, rx) = mpsc::channel(1);
        receivers.push(rx);
        let web_state = progress.web_state.clone();
        let abort = abort.clone();
        let shutdown = shutdown.clone();

        tasks.spawn(async move {
            // Striped assignment: this worker owns chunks worker, worker+n, ...
            // Each chunk is fetched with its own stream so a short read (which
            // ends grammers' stream early) only affects that one piece.
            let mut idx = start_chunk + worker;
            while idx < total_chunks {
                if abort.load(Ordering::Relaxed) || shutdown.is_cancelled() {
                    break;
                }
                if !wait_paused(&web_state, &shutdown).await {
                    break;
                }
                let offset = idx * chunk_size;
                let expected = (total - offset).min(chunk_size);
                match fetch_chunk(&client, &media, idx, expected, &web_state, &shutdown).await {
                    Ok(chunk) => {
                        if send_chunk(&tx, chunk, &shutdown).await.is_err() {
                            break; // receiver gone or shutdown requested
                        }
                    }
                    Err(e) => {
                        abort.store(true, Ordering::Relaxed);
                        return Err(e);
                    }
                }
                idx += workers;
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    // Invalidate an old checkpoint before truncating or preallocating a fresh
    // file, so an interruption cannot make old progress describe new bytes.
    if start == 0 {
        write_progress(path, 0, &progress.web_state.database).await?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(start == 0)
        .open(path)
        .await?;
    // Preallocate the full file up front. On a NAS this avoids growing the
    // file extent-by-extent as chunks land, which fragments the layout and
    // can stall writes. Falls back to a sparse truncate if unsupported.
    preallocate(&file, total).await?;
    file.seek(std::io::SeekFrom::Start(start)).await?;

    let mut next = start;
    let mut last_flushed = start;
    let started = Instant::now();
    let mut last_reported = start;
    let mut last_reported_at = Instant::now();
    while next < total {
        let worker = ((next / chunk_size - start_chunk) % workers) as usize;
        // Race the next ordered chunk against shutdown, keeping the written
        // prefix contiguous even if another worker has already fetched ahead.
        let chunk = tokio::select! {
            m = receivers[worker].recv() => match m {
                Some(chunk) => chunk,
                None => break,
            },
            _ = shutdown.cancelled() => break,
        };
        file.write_all(&chunk).await?;
        next += chunk.len() as u64;
        if last_reported_at.elapsed() >= PROGRESS_REPORT_INTERVAL {
            report_download_progress(progress, start, next, started).await;
            last_reported = next;
            last_reported_at = Instant::now();
        }
        // The file is preallocated, so only this durable contiguous position
        // can tell a later download where to resume.
        if next - last_flushed >= PROGRESS_FLUSH_BYTES {
            file.flush().await?;
            file.sync_data().await?;
            write_progress(path, next, &progress.web_state.database).await?;
            last_flushed = next;
        }
    }
    // Release blocked senders before joining after cancellation or a failed chunk.
    drop(receivers);
    if last_reported != next {
        report_download_progress(progress, start, next, started).await;
    }
    // Final flush: persist the full contiguous position (== `total` on success)
    // so a later run can finalize without re-fetching.
    file.flush().await?;
    file.sync_data().await?;
    write_progress(path, next, &progress.web_state.database).await?;

    // Surface the first worker error (a chunk that exhausted its retries).
    let mut worker_err: Option<anyhow::Error> = None;
    let mut cancelled = shutdown.is_cancelled();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) if worker_err.is_none() => worker_err = Some(e),
            Ok(Err(_)) => {}
            Err(e) if e.is_cancelled() => cancelled = true,
            Err(e) if worker_err.is_none() => {
                worker_err = Some(anyhow::anyhow!("download worker join failed: {e}"))
            }
            Err(_) => {}
        }
    }
    if cancelled {
        // Leave the contiguous prefix on disk for resume; report interruption
        // so the caller keeps the `.part` file rather than deleting it.
        return Err(anyhow::anyhow!("download interrupted"));
    }
    if let Some(e) = worker_err {
        return Err(e);
    }
    // No worker errored, so every chunk must have been written contiguously.
    // A gap here would indicate a logic bug rather than a network failure.
    if next != total {
        return Err(anyhow::anyhow!(
            "incomplete download: wrote {next} of {total} bytes"
        ));
    }

    Ok(())
}

/// Fetch a single chunk at index `idx`, retrying transient failures.
///
/// A fresh `iter_download` stream is used per attempt. This matters because
/// grammers marks its stream done as soon as a read returns fewer bytes than
/// requested (see grammers `files.rs`); a transient short read would otherwise
/// end the stream and starve the rest of the fetch. Each chunk is also
/// validated against its expected size so a short piece is never written.
///
/// The fetch and its retry backoff are both raced against `shutdown` so a
/// Ctrl+C cancels the in-flight network read at once instead of waiting for
/// it (or its backoff) to finish.
async fn fetch_chunk(
    client: &Client,
    media: &Media,
    idx: u64,
    expected: u64,
    web_state: &Arc<ApiState>,
    shutdown: &Shutdown,
) -> anyhow::Result<Vec<u8>> {
    let mut backoff = 0u64;
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..CHUNK_RETRY_LIMIT {
        if shutdown.is_cancelled() {
            break;
        }
        let delay = std::mem::take(&mut backoff);
        if delay > 0 && sleep_cancellable(shutdown, Duration::from_secs(delay)).await {
            break;
        }
        // Keep the on-disk checkpoint size unchanged, but request smaller
        // pieces at low rates so parallel workers cannot bypass the budget.
        let request_size = web_state
            .download_limiter
            .limit(DownloadModule::Telegram)
            .map_or(DOWNLOAD_CHUNK_SIZE, |rate| {
                (rate / 10)
                    .clamp(4096, DOWNLOAD_CHUNK_SIZE)
                    .next_power_of_two()
            });
        let mut stream = client
            .iter_download(media)
            .chunk_size(request_size as i32)
            .skip_chunks(i32::try_from(idx * (DOWNLOAD_CHUNK_SIZE / request_size))?);
        let result: anyhow::Result<Option<Vec<u8>>> = tokio::select! {
            r = async {
                let mut data = Vec::with_capacity(expected as usize);
                while (data.len() as u64) < expected {
                    let amount = request_size.min(expected - data.len() as u64);
                    web_state.download_limiter.acquire(DownloadModule::Telegram, amount as usize).await;
                    anyhow::ensure!(wait_paused(web_state, shutdown).await, "download interrupted");
                    let Some(chunk) = stream.next().await? else { break; };
                    let short = (chunk.len() as u64) < amount;
                    data.extend_from_slice(&chunk);
                    if short { break; }
                }
                Ok(Some(data))
            } => r,
            _ = shutdown.cancelled() => break,
        };
        match result {
            Ok(Some(chunk)) if chunk.len() as u64 == expected => return Ok(chunk),
            Ok(Some(chunk)) => {
                warn!(
                    "chunk {idx}: short read {} B (expected {expected}), retry {}/{}",
                    chunk.len(),
                    attempt + 1,
                    CHUNK_RETRY_LIMIT
                );
                last_err = Some(anyhow::anyhow!(
                    "chunk {idx} short: {} vs {expected} bytes",
                    chunk.len()
                ));
                backoff = RETRY_DELAY_SECS;
            }
            Ok(None) => {
                warn!(
                    "chunk {idx}: stream ended early, retry {}/{}",
                    attempt + 1,
                    CHUNK_RETRY_LIMIT
                );
                last_err = Some(anyhow::anyhow!("chunk {idx} stream ended early"));
                backoff = RETRY_DELAY_SECS;
            }
            Err(e) => {
                backoff = flood_wait_secs(&e.to_string()).unwrap_or(RETRY_DELAY_SECS);
                warn!(
                    "chunk {idx}: fetch error: {e}; retry {}/{} in {backoff}s",
                    attempt + 1,
                    CHUNK_RETRY_LIMIT
                );
                last_err = Some(anyhow::anyhow!("chunk {idx}: {e}"));
            }
        }
    }
    if shutdown.is_cancelled() {
        return Err(anyhow::anyhow!("download interrupted"));
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("chunk {idx} failed after retries")))
}

/// Unknown-size media cannot use the resumable chunk writer, but still shares
/// the bandwidth budget and responds to pause/shutdown between requests.
pub(super) async fn download_unknown_size(
    client: &Client,
    media: &Media,
    path: &Path,
    progress: &DownloadProgress<'_>,
    shutdown: &Shutdown,
) -> anyhow::Result<()> {
    let mut file = tokio::fs::File::create(path).await?;
    let request_size = progress
        .web_state
        .download_limiter
        .limit(DownloadModule::Telegram)
        .map_or(DOWNLOAD_CHUNK_SIZE, |rate| {
            (rate / 10)
                .clamp(4096, DOWNLOAD_CHUNK_SIZE)
                .next_power_of_two()
        });
    let mut stream = client.iter_download(media).chunk_size(request_size as i32);
    let mut downloaded = 0;
    let started = Instant::now();
    loop {
        let chunk = tokio::select! {
            result = async {
                progress.web_state.download_limiter.acquire(DownloadModule::Telegram, request_size as usize).await;
                anyhow::ensure!(wait_paused(progress.web_state, shutdown).await, "download interrupted");
                Ok::<_, anyhow::Error>(stream.next().await?)
            } => result?,
            _ = shutdown.cancelled() => anyhow::bail!("download interrupted"),
        };
        let Some(chunk) = chunk else {
            break;
        };
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        report_download_progress(progress, 0, downloaded, started).await;
    }
    file.flush().await?;
    file.sync_data().await?;
    Ok(())
}
