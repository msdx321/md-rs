use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures_util::{Stream, StreamExt};
use grammers_client::Client;
use grammers_client::media::Media;
use log::{debug, trace};
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
    let msg_id = progress.msg_id;
    debug!(
        "msg={msg_id}: transfer start offset={start} total={total} chunks={total_chunks} workers={workers}"
    );

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
                match fetch_chunk(
                    &client, &media, msg_id, idx, expected, &web_state, &shutdown,
                )
                .await
                {
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

    let next = write_ordered_chunks(path, range, receivers, progress, shutdown).await?;

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

/// Consume bounded worker queues in stripe order and checkpoint only the synced prefix.
async fn write_ordered_chunks(
    path: &Path,
    range: std::ops::Range<u64>,
    mut receivers: Vec<mpsc::Receiver<Vec<u8>>>,
    progress: &DownloadProgress<'_>,
    shutdown: &Shutdown,
) -> anyhow::Result<u64> {
    let start = range.start;
    let total = range.end;
    let chunk_size = DOWNLOAD_CHUNK_SIZE;
    let start_chunk = start / chunk_size;
    let workers = receivers.len() as u64;
    let msg_id = progress.msg_id;
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
    let outcome: anyhow::Result<()> = async {
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
            trace!(
                "msg={msg_id}: wrote chunk bytes={} offset={next}/{total}",
                chunk.len()
            );
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
                trace!("msg={msg_id}: durable checkpoint offset={next}/{total}");
                last_flushed = next;
            }
        }
        Ok(())
    }
    .await;
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
    debug!(
        "msg={msg_id}: transfer stopped offset={next}/{total} elapsed_ms={}",
        started.elapsed().as_millis()
    );

    outcome?;
    Ok(next)
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
    msg_id: i32,
    idx: u64,
    expected: u64,
    web_state: &Arc<ApiState>,
    shutdown: &Shutdown,
) -> anyhow::Result<Vec<u8>> {
    fetch_chunk_with(
        msg_id,
        idx,
        expected,
        web_state,
        shutdown,
        |request_size, skip| {
            let stream = client
                .iter_download(media)
                .chunk_size(request_size as i32)
                .skip_chunks(skip);
            futures_util::stream::try_unfold(stream, |mut stream| async move {
                Ok::<_, anyhow::Error>(stream.next().await?.map(|chunk| (chunk, stream)))
            })
        },
    )
    .await
}

/// The factory is the transport boundary: each retry opens a new reader. Budget,
/// pause, size validation, retry limits and cancellation stay in this one loop.
async fn fetch_chunk_with<S: Stream<Item = anyhow::Result<Vec<u8>>>>(
    msg_id: i32,
    idx: u64,
    expected: u64,
    web_state: &Arc<ApiState>,
    shutdown: &Shutdown,
    mut open: impl FnMut(u64, i32) -> S,
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
        trace!(
            "msg={msg_id}: fetching chunk={idx} attempt={}/{} expected_bytes={expected} request_bytes={request_size}",
            attempt + 1,
            CHUNK_RETRY_LIMIT
        );
        let stream = open(
            request_size,
            i32::try_from(idx * (DOWNLOAD_CHUNK_SIZE / request_size))?,
        );
        tokio::pin!(stream);
        let result: anyhow::Result<Option<Vec<u8>>> = tokio::select! {
            r = async {
                let mut data = Vec::with_capacity(expected as usize);
                while (data.len() as u64) < expected {
                    let amount = request_size.min(expected - data.len() as u64);
                    web_state.download_limiter.acquire(DownloadModule::Telegram, amount as usize).await;
                    anyhow::ensure!(wait_paused(web_state, shutdown).await, "download interrupted");
                    let Some(chunk) = stream.next().await.transpose()? else { break; };
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
                debug!(
                    "msg={msg_id}: chunk {idx}: short read {} B (expected {expected}), attempt {}/{}",
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
                debug!(
                    "msg={msg_id}: chunk {idx}: stream ended early, attempt {}/{}",
                    attempt + 1,
                    CHUNK_RETRY_LIMIT
                );
                last_err = Some(anyhow::anyhow!("chunk {idx} stream ended early"));
                backoff = RETRY_DELAY_SECS;
            }
            Err(e) => {
                backoff = flood_wait_secs(&e.to_string()).unwrap_or(RETRY_DELAY_SECS);
                debug!(
                    "msg={msg_id}: chunk {idx}: fetch error: {e}; attempt {}/{} backoff={backoff}s",
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
    let msg_id = progress.msg_id;
    debug!("msg={msg_id}: starting non-resumable transfer (unknown size)");
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
    let outcome: anyhow::Result<()> = async {
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
        trace!(
            "msg={msg_id}: wrote chunk bytes={} downloaded={downloaded}",
            chunk.len()
        );
        report_download_progress(progress, 0, downloaded, started).await;
    }
    Ok(())
    }.await;
    file.flush().await?;
    file.sync_data().await?;
    debug!(
        "msg={msg_id}: transfer ended success={} bytes={downloaded} elapsed_ms={}",
        outcome.is_ok(),
        started.elapsed().as_millis()
    );
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Database;
    use crate::telegram::downloader::progress::resume_offset;

    async fn state(database: Database) -> Arc<ApiState> {
        let common = tokio::sync::watch::channel(crate::configuration::app::Config::default()).1;
        Arc::new(
            ApiState::new(
                mpsc::channel(1).0,
                database,
                common.clone(),
                Arc::new(crate::runtime::download_limiter::DownloadLimiter::new(
                    common,
                )),
            )
            .await
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn ordered_writer_reuses_prefix_and_joins_out_of_order_stripes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("media.part");
        let chunk = DOWNLOAD_CHUNK_SIZE as usize;
        let total = 4 * DOWNLOAD_CHUNK_SIZE + 7;
        let mut expected = vec![9; chunk];
        expected.extend(vec![1; chunk]);
        expected.extend(vec![2; chunk]);
        expected.extend(vec![3; chunk]);
        expected.extend(vec![4; 7]);
        tokio::fs::write(&path, vec![9; chunk]).await.unwrap();
        let state = state(Database::open(":memory:").await.unwrap()).await;
        write_progress(&path, DOWNLOAD_CHUNK_SIZE, &state.database)
            .await
            .unwrap();
        let pb = indicatif::ProgressBar::hidden();
        let progress = DownloadProgress {
            pb: &pb,
            web_state: &state,
            msg_id: 1,
        };
        let shutdown = Shutdown::new();
        let (first, first_rx) = mpsc::channel(1);
        let (second, second_rx) = mpsc::channel(1);
        // Later stripe arrives first; its bounded queue cannot accept another chunk.
        second.send(vec![2; chunk]).await.unwrap();
        let ahead = send_chunk(&second, vec![4; 7], &shutdown);
        tokio::pin!(ahead);
        assert!(futures_util::poll!(&mut ahead).is_pending());
        let writer = write_ordered_chunks(
            &path,
            DOWNLOAD_CHUNK_SIZE..total,
            vec![first_rx, second_rx],
            &progress,
            &shutdown,
        );
        let producer = async {
            first.send(vec![1; chunk]).await.unwrap();
            first.send(vec![3; chunk]).await.unwrap();
            ahead.await.unwrap();
        };
        let (written, ()) = tokio::join!(writer, producer);
        assert_eq!(written.unwrap(), total);
        assert_eq!(tokio::fs::read(&path).await.unwrap(), expected);
        assert_eq!(
            resume_offset(&path, total, &state.database).await.unwrap(),
            total
        );
    }

    #[tokio::test]
    async fn closed_stripe_checkpoints_only_contiguous_bytes_not_preallocated_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("media.part");
        let total = 3 * DOWNLOAD_CHUNK_SIZE;
        let state = state(Database::open(dir.path().join("state.db")).await.unwrap()).await;
        let pb = indicatif::ProgressBar::hidden();
        let progress = DownloadProgress {
            pb: &pb,
            web_state: &state,
            msg_id: 1,
        };
        let (tx, rx) = mpsc::channel(1);
        tx.send(vec![7; DOWNLOAD_CHUNK_SIZE as usize])
            .await
            .unwrap();
        drop(tx); // Equivalent writer input to a worker exhausting its retries.
        assert_eq!(
            write_ordered_chunks(&path, 0..total, vec![rx], &progress, &Shutdown::new())
                .await
                .unwrap(),
            DOWNLOAD_CHUNK_SIZE
        );
        assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), total);
        assert_eq!(
            resume_offset(&path, total, &state.database).await.unwrap(),
            DOWNLOAD_CHUNK_SIZE
        );
        let bytes = tokio::fs::read(&path).await.unwrap();
        assert!(
            bytes[..DOWNLOAD_CHUNK_SIZE as usize]
                .iter()
                .all(|b| *b == 7)
        );
        assert!(
            bytes[DOWNLOAD_CHUNK_SIZE as usize..]
                .iter()
                .all(|b| *b == 0)
        );
    }

    #[tokio::test]
    async fn cancelled_writer_retains_checkpoint_and_releases_blocked_sender() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("media.part");
        let total = 2 * DOWNLOAD_CHUNK_SIZE;
        tokio::fs::write(&path, vec![3; DOWNLOAD_CHUNK_SIZE as usize])
            .await
            .unwrap();
        let state = state(Database::open(":memory:").await.unwrap()).await;
        let pb = indicatif::ProgressBar::hidden();
        let progress = DownloadProgress {
            pb: &pb,
            web_state: &state,
            msg_id: 1,
        };
        let shutdown = Shutdown::new();
        let (tx, rx) = mpsc::channel(1);
        // No queued data: cancellation deterministically wins the waiting read.
        shutdown.cancel();
        assert_eq!(
            write_ordered_chunks(
                &path,
                DOWNLOAD_CHUNK_SIZE..total,
                vec![rx],
                &progress,
                &shutdown
            )
            .await
            .unwrap(),
            DOWNLOAD_CHUNK_SIZE
        );
        assert!(tx.is_closed());
        assert_eq!(
            resume_offset(&path, total, &state.database).await.unwrap(),
            DOWNLOAD_CHUNK_SIZE
        );
        let (tx, _rx) = mpsc::channel(1);
        tx.send(vec![1]).await.unwrap();
        assert_eq!(
            send_chunk(&tx, vec![2], &shutdown).await.unwrap_err().0,
            vec![2]
        );
    }

    #[tokio::test]
    async fn active_ordered_writer_cancellation_syncs_exact_prefix_and_releases_ahead_sender() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("active.part");
        let state = state(Database::open(":memory:").await.unwrap()).await;
        let pb = indicatif::ProgressBar::hidden();
        let progress = DownloadProgress {
            pb: &pb,
            web_state: &state,
            msg_id: 9,
        };
        let shutdown = Shutdown::new();
        let (even, even_rx) = mpsc::channel(1);
        let (odd, odd_rx) = mpsc::channel(1);
        let total = PROGRESS_FLUSH_BYTES + 4 * DOWNLOAD_CHUNK_SIZE;
        let writer =
            write_ordered_chunks(&path, 0..total, vec![even_rx, odd_rx], &progress, &shutdown);
        let produce_even = async {
            for _ in 0..PROGRESS_FLUSH_BYTES / DOWNLOAD_CHUNK_SIZE / 2 {
                even.send(vec![6; DOWNLOAD_CHUNK_SIZE as usize])
                    .await
                    .unwrap();
            }
            // Keep this sender alive but never supply the next ordered chunk.
        };
        let produce_odd = async {
            for _ in 0..PROGRESS_FLUSH_BYTES / DOWNLOAD_CHUNK_SIZE / 2 {
                odd.send(vec![7; DOWNLOAD_CHUNK_SIZE as usize])
                    .await
                    .unwrap();
            }
            odd.send(vec![8; DOWNLOAD_CHUNK_SIZE as usize])
                .await
                .unwrap();
            let blocked = send_chunk(&odd, vec![9; DOWNLOAD_CHUNK_SIZE as usize], &shutdown);
            tokio::pin!(blocked);
            assert!(futures_util::poll!(&mut blocked).is_pending());
            // The durable checkpoint is a production gate *after* 32 actual writes.
            // The next ordered queue is empty, so the writer cannot advance past it.
            while crate::migration::resume_offset(&path, &state.database)
                .await
                .unwrap()
                != PROGRESS_FLUSH_BYTES
            {
                tokio::task::yield_now().await;
            }
            shutdown.cancel();
            assert!(blocked.await.is_err());
        };
        let (written, (), ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(writer, produce_even, produce_odd)
        })
        .await
        .unwrap();
        assert_eq!(written.unwrap(), PROGRESS_FLUSH_BYTES);
        assert!(even.is_closed() && odd.is_closed());
        assert_eq!(
            resume_offset(&path, total, &state.database).await.unwrap(),
            PROGRESS_FLUSH_BYTES
        );
        let bytes = tokio::fs::read(&path).await.unwrap();
        for (idx, chunk) in bytes[..PROGRESS_FLUSH_BYTES as usize]
            .as_chunks::<{ DOWNLOAD_CHUNK_SIZE as usize }>()
            .0
            .iter()
            .enumerate()
        {
            assert!(chunk.iter().all(|b| *b == if idx % 2 == 0 { 6 } else { 7 }));
        }
        assert!(
            bytes[PROGRESS_FLUSH_BYTES as usize..]
                .iter()
                .all(|b| *b == 0)
        );
    }

    #[tokio::test]
    async fn fetch_chunk_retries_fresh_readers_after_short_and_transient_reads() {
        let state = state(Database::open(":memory:").await.unwrap()).await;
        let mut starts = vec![];
        let mut attempt = 0;
        let bytes = fetch_chunk_with(1, 3, 4, &state, &Shutdown::new(), |size, skip| {
            starts.push((size, skip, Instant::now()));
            attempt += 1;
            let result = match attempt {
                1 => Ok(vec![1, 2]),
                2 => Err(anyhow::anyhow!("transient fixture failure")),
                3 => Ok(vec![9; 4]),
                _ => panic!("retry limit exceeded"),
            };
            futures_util::stream::iter([result])
        })
        .await
        .unwrap();
        assert_eq!(bytes, vec![9; 4]);
        assert_eq!(starts.len(), 3);
        for entry in &starts {
            assert_eq!((entry.0, entry.1), (DOWNLOAD_CHUNK_SIZE, 3));
        }
        for pair in starts.windows(2) {
            assert!(pair[1].2.duration_since(pair[0].2) >= Duration::from_secs(RETRY_DELAY_SECS));
        }
    }

    #[tokio::test]
    async fn fetch_chunk_exhausts_exactly_three_attempts_on_empty_and_failed_readers() {
        let state = state(Database::open(":memory:").await.unwrap()).await;
        let mut attempts = 0;
        let start = Instant::now();
        let error = fetch_chunk_with(1, 0, 4, &state, &Shutdown::new(), |_, _| {
            attempts += 1;
            futures_util::stream::iter(if attempts == 1 {
                vec![]
            } else {
                vec![Err(anyhow::anyhow!("failed {attempts}"))]
            })
        })
        .await
        .unwrap_err();
        assert_eq!(attempts, CHUNK_RETRY_LIMIT);
        assert!(error.to_string().contains("failed 3"));
        assert!(start.elapsed() >= Duration::from_secs(2 * RETRY_DELAY_SECS));
    }

    #[tokio::test]
    async fn fetch_chunk_flood_wait_uses_server_backoff_then_recovers() {
        let state = state(Database::open(":memory:").await.unwrap()).await;
        let mut attempts = 0;
        let mut start = None;
        let bytes = fetch_chunk_with(1, 0, 1, &state, &Shutdown::new(), |_, _| {
            attempts += 1;
            let result = if attempts == 1 {
                start = Some(Instant::now());
                Err(anyhow::anyhow!("FLOOD_WAIT_1"))
            } else {
                let elapsed = start.unwrap().elapsed();
                assert!(elapsed >= Duration::from_secs(1));
                // Ordinary retry delay is 5s; this must use the server's 1s value.
                assert!(elapsed < Duration::from_secs(RETRY_DELAY_SECS));
                Ok(vec![3])
            };
            futures_util::stream::iter([result])
        })
        .await
        .unwrap();
        assert_eq!(bytes, vec![3]);
        assert_eq!(attempts, 2);
    }

    #[tokio::test]
    async fn fetch_chunk_cancels_active_fetch_and_backoff_without_reopening() {
        let state = state(Database::open(":memory:").await.unwrap()).await;
        for active_fetch in [true, false] {
            let shutdown = Shutdown::new();
            let (entered, mut entered_rx) = mpsc::unbounded_channel();
            let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut attempts = 0;
            let fetch = fetch_chunk_with(1, 0, 4, &state, &shutdown, |_, _| {
                attempts += 1;
                let entered = entered.clone();
                let reads = reads.clone();
                futures_util::stream::poll_fn(move |_| {
                    if reads.fetch_add(1, Ordering::Relaxed) == 0 {
                        entered.send(()).unwrap();
                    }
                    if active_fetch {
                        std::task::Poll::Pending
                    } else {
                        std::task::Poll::Ready(Some(Err(anyhow::anyhow!("FLOOD_WAIT_60"))))
                    }
                })
            });
            let cancel = async {
                entered_rx.recv().await.unwrap();
                // fetch has yielded either inside next() or the actual retry sleep.
                shutdown.cancel();
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async {
                tokio::join!(fetch, cancel)
            })
            .await
            .unwrap();
            assert!(result.unwrap_err().to_string().contains("interrupted"));
            assert_eq!(attempts, 1);
            assert!(reads.load(Ordering::Relaxed) > 0);
        }
    }

    #[tokio::test]
    async fn fetch_chunk_applies_request_sizing_striped_offset_and_real_bandwidth_budget() {
        let config = crate::configuration::app::Config {
            telegram_download_limit_mb_per_sec: 0.08192,
            ..Default::default()
        };
        let (_settings, common) = tokio::sync::watch::channel(config);
        let state = Arc::new(
            ApiState::new(
                mpsc::channel(1).0,
                Database::open(":memory:").await.unwrap(),
                common.clone(),
                Arc::new(crate::runtime::download_limiter::DownloadLimiter::new(
                    common,
                )),
            )
            .await
            .unwrap(),
        );
        let mut attempts = 0;
        let started = Instant::now();
        let bytes = fetch_chunk_with(1, 3, 16391, &state, &Shutdown::new(), |size, skip| {
            attempts += 1;
            assert_eq!(size, 8192);
            assert_eq!(skip, 3 * 64);
            futures_util::stream::iter([Ok(vec![1; 8192]), Ok(vec![2; 8192]), Ok(vec![3; 7])])
        })
        .await
        .unwrap();
        assert_eq!(attempts, 1);
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "fixture bytes must not bypass the real limiter"
        );
        assert_eq!(bytes, [vec![1; 8192], vec![2; 8192], vec![3; 7]].concat());
    }

    #[tokio::test]
    async fn fetch_chunk_cancellation_interrupts_budget_before_transport_reads() {
        let config = crate::configuration::app::Config {
            telegram_download_limit_mb_per_sec: 0.000001,
            ..Default::default()
        };
        let (_settings, common) = tokio::sync::watch::channel(config);
        let state = Arc::new(
            ApiState::new(
                mpsc::channel(1).0,
                Database::open(":memory:").await.unwrap(),
                common.clone(),
                Arc::new(crate::runtime::download_limiter::DownloadLimiter::new(
                    common,
                )),
            )
            .await
            .unwrap(),
        );
        let shutdown = Shutdown::new();
        let opened = std::cell::Cell::new(0);
        let reads = std::cell::Cell::new(0);
        let fetch = fetch_chunk_with(1, 2, 4, &state, &shutdown, |size, skip| {
            opened.set(opened.get() + 1);
            assert_eq!((size, skip), (4096, 256));
            futures_util::stream::poll_fn(|_| {
                reads.set(reads.get() + 1);
                std::task::Poll::Ready(Some(Ok(vec![1; 4])))
            })
        });
        tokio::pin!(fetch);
        assert!(futures_util::poll!(&mut fetch).is_pending());
        assert_eq!(opened.get(), 1);
        assert_eq!(reads.get(), 0);
        shutdown.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), fetch)
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("interrupted")
        );
        assert_eq!(reads.get(), 0);
    }
}
