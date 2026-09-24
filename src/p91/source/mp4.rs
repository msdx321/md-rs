//! Read MP4 track dimensions without downloading `mdat`. Bounded range requests
//! use the same credentials, transport and bandwidth budget as the download.

use futures_util::StreamExt;

use super::http::Fetcher;
use crate::runtime::download_limiter::{DownloadLimiter, DownloadModule};

pub async fn dimensions(
    fetch: &Fetcher,
    url: &str,
    origin: &str,
    limiter: &DownloadLimiter,
) -> anyhow::Result<Option<(u64, u64)>> {
    let mut offset = 0u64;
    // Avoid unbounded requests or allocations on malformed/unusual containers.
    for _ in 0..32 {
        let bytes = read_range(fetch, url, origin, offset, 16, limiter).await?;
        let (size, header, kind) =
            box_header(&bytes).ok_or_else(|| anyhow::anyhow!("unsupported MP4 box header"))?;
        let next = offset
            .checked_add(size)
            .ok_or_else(|| anyhow::anyhow!("MP4 box offset overflow"))?;
        if kind == b"moov" {
            anyhow::ensure!(size <= 4 * 1024 * 1024, "MP4 metadata exceeds 4 MiB");
            if size == header as u64 {
                return Ok(None);
            }
            let body = read_range(
                fetch,
                url,
                origin,
                offset + header as u64,
                size as usize - header,
                limiter,
            )
            .await?;
            return Ok(track_dimensions(&body));
        }
        offset = next;
    }
    anyhow::bail!("MP4 metadata not found within 32 boxes")
}

async fn read_range(
    fetch: &Fetcher,
    url: &str,
    origin: &str,
    start: u64,
    length: usize,
    limiter: &DownloadLimiter,
) -> anyhow::Result<Vec<u8>> {
    let end = start
        .checked_add(length as u64)
        .and_then(|end| end.checked_sub(1))
        .ok_or_else(|| anyhow::anyhow!("invalid MP4 metadata range"))?;
    let response = fetch
        .media_response(url, origin, Some((start, Some(end))))
        .await?;
    if response.status() == wreq::StatusCode::PARTIAL_CONTENT {
        let returned_start = response
            .headers()
            .get("content-range")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("bytes "))
            .and_then(|value| value.split_once('-'))
            .and_then(|(value, _)| value.parse::<u64>().ok());
        anyhow::ensure!(
            returned_start == Some(start),
            "incorrect MP4 metadata range"
        );
    } else {
        anyhow::ensure!(
            start == 0 && response.status() == wreq::StatusCode::OK,
            "server does not support MP4 metadata ranges"
        );
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::with_capacity(length);
    while bytes.len() < length {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let chunk = chunk?;
        limiter.acquire(DownloadModule::P91, chunk.len()).await;
        let take = chunk.len().min(length - bytes.len());
        bytes.extend_from_slice(&chunk[..take]);
    }
    anyhow::ensure!(bytes.len() == length, "truncated MP4 metadata");
    Ok(bytes)
}

fn box_header(bytes: &[u8]) -> Option<(u64, usize, &[u8])> {
    let size = u32::from_be_bytes(bytes.get(..4)?.try_into().ok()?);
    let kind = bytes.get(4..8)?;
    let (size, header) = if size == 1 {
        (u64::from_be_bytes(bytes.get(8..16)?.try_into().ok()?), 16)
    } else {
        (u64::from(size), 8)
    };
    (size >= header as u64).then_some((size, header, kind))
}

fn boxes(mut bytes: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> {
    std::iter::from_fn(move || {
        let (size, header, kind) = box_header(bytes)?;
        let size = usize::try_from(size).ok()?;
        let body = bytes.get(header..size)?;
        bytes = bytes.get(size..)?;
        Some((kind, body))
    })
}

fn track_dimensions(moov: &[u8]) -> Option<(u64, u64)> {
    boxes(moov)
        .filter(|(kind, _)| *kind == b"trak")
        .filter_map(|(_, track)| {
            let media = boxes(track).find(|(kind, _)| *kind == b"mdia")?.1;
            let handler = boxes(media).find(|(kind, _)| *kind == b"hdlr")?.1;
            if handler.get(8..12)? != b"vide" {
                return None;
            }
            let header = boxes(track).find(|(kind, _)| *kind == b"tkhd")?.1;
            let offset = match header.first()? {
                0 => 76,
                1 => 88,
                _ => return None,
            };
            // Track header dimensions are unsigned 16.16 fixed-point values.
            let width = u32::from_be_bytes(header.get(offset..offset + 4)?.try_into().ok()?) >> 16;
            let height =
                u32::from_be_bytes(header.get(offset + 4..offset + 8)?.try_into().ok()?) >> 16;
            (width > 0 && height > 0).then_some((u64::from(width), u64::from(height)))
        })
        .max_by_key(|(width, height)| (*width).min(*height))
}
