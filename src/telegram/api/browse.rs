//! Read-only photo/video browsing over the downloader's authorized client.
use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use grammers_client::media::{Downloadable, Media, PhotoSize};
use grammers_client::{Client, tl};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Semaphore};

use super::{ApiState, ChatTarget};
use crate::telegram::downloader::{media_duration_value, media_file_size_value};

const PAGE_SIZE: usize = 24;
const MAX_THUMB_BYTES: usize = 128 * 1024;
const MAX_THUMBS: usize = PAGE_SIZE * 10;

type ApiError = (StatusCode, Json<serde_json::Value>);

pub(crate) struct Session {
    client: Client,
    file_ids: Arc<Mutex<FxHashMap<String, u64>>>,
    thumbnails: Mutex<BTreeMap<u64, PhotoSize>>,
    /// Thumbnail downloads queue here. Page listings have their own permits so
    /// a page of loading thumbnails can never make the next listing "busy".
    requests: Semaphore,
    listings: Semaphore,
}

impl ApiState {
    /// The downloader owns the session. Its exit invalidates browsing too.
    pub(crate) async fn enable_browsing(
        &self,
        client: Client,
        file_ids: Arc<Mutex<FxHashMap<String, u64>>>,
    ) -> Arc<Session> {
        let session = Arc::new(Session {
            client,
            file_ids,
            thumbnails: Mutex::new(BTreeMap::new()),
            requests: Semaphore::new(4),
            listings: Semaphore::new(2),
        });
        *self.browse.lock().await = Arc::downgrade(&session);
        session
    }
}

async fn session(state: &ApiState) -> Result<Arc<Session>, ApiError> {
    state.browse.lock().await.upgrade().ok_or_else(|| {
        error(
            StatusCode::CONFLICT,
            "Connect Telegram before browsing chats",
        )
    })
}

fn error(status: StatusCode, message: &str) -> ApiError {
    (status, Json(serde_json::json!({ "error": message })))
}

#[derive(Deserialize)]
pub(super) struct BrowseQuery {
    chat: String,
    #[serde(default)]
    before: i32,
}

#[derive(Serialize)]
struct Card {
    message_id: i32,
    caption: String,
    date: String,
    media_type: &'static str,
    size: i64,
    duration_secs: i64,
    image_url: Option<String>,
    downloaded: bool,
}

fn thumbnail(media: &Media) -> Option<PhotoSize> {
    let thumbs = match media {
        Media::Photo(photo) => photo.thumbs(),
        Media::Document(document) => document.thumbs(),
        _ => return None,
    };
    thumbs
        .into_iter()
        .filter(|thumb| !matches!(thumb, PhotoSize::Empty(_) | PhotoSize::Path(_)))
        .filter(|thumb| (1..=MAX_THUMB_BYTES).contains(&thumb.size()))
        .max_by_key(|thumb| thumb.size())
}

pub(super) async fn list(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<BrowseQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if query.before < 0 || query.chat.is_empty() || query.chat.len() > 512 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "Invalid chat or page cursor",
        ));
    }
    let chat = query.chat.trim().trim_start_matches('@');
    if chat.parse::<i64>().is_err() && !super::valid_username(chat) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "Choose a subscribed chat or enter a username or numeric chat ID",
        ));
    }
    let session = session(&state).await?;
    let _permit = session.listings.try_acquire().map_err(|_| {
        error(
            StatusCode::TOO_MANY_REQUESTS,
            "Telegram preview is busy. Try again shortly.",
        )
    })?;
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        // Resolve only existing/public chats. Never accept invite links here.
        let (peer, name) = crate::telegram::app::scan::resolve_chat(
            &session.client,
            chat,
            &mut crate::telegram::app::scan::DialogLookup::new(&session.client),
        )
        .await?;
        let mut cfg = crate::telegram::config::FILE
            .load_optional_off_thread()
            .await?
            .unwrap_or_default();
        cfg.save_path = state.common.borrow().telegram_download_path.clone();
        let mut messages = session
            .client
            .search_messages(peer)
            .filter(tl::enums::MessagesFilter::InputMessagesFilterPhotoVideo)
            .offset_id(query.before)
            .limit(PAGE_SIZE + 1);
        let mut cards = Vec::new();
        let mut next_before = None;
        while let Some(message) = messages.next().await? {
            if cards.len() == PAGE_SIZE {
                next_before = cards.last().map(|card: &Card| card.message_id);
                break;
            }
            let Some(media) = message.media() else {
                continue;
            };
            let (fid, media_type) = match &media {
                Media::Photo(photo) => (photo.id().to_string(), "photo"),
                Media::Document(document) => (document.id().to_string(), "video"),
                _ => continue,
            };
            let mut downloaded = session
                .file_ids
                .lock()
                .await
                .get(&fid)
                .is_some_and(|time| *time > state.history_cutoff());
            if downloaded {
                let paths =
                    crate::telegram::downloader::paths::build_media_paths(&message, &media, &cfg)?;
                downloaded = !crate::telegram::storage::history::was_forgotten(
                    &*state.database.connection().await,
                    &paths.final_path.to_string_lossy(),
                )
                .await?;
            }
            let image_url = if let Some(thumb) = thumbnail(&media) {
                let id = state
                    .thumbnail_id
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut cache = session.thumbnails.lock().await;
                cache.insert(id, thumb);
                while cache.len() > MAX_THUMBS {
                    cache.pop_first();
                }
                Some(format!("/telegram/api/thumbnails/{id}"))
            } else {
                None
            };
            cards.push(Card {
                message_id: message.id(),
                caption: message.text().chars().take(1024).collect(),
                date: message.date().to_rfc3339(),
                media_type,
                size: media_file_size_value(&message),
                duration_secs: media_duration_value(&message),
                image_url,
                downloaded,
            });
        }
        Ok::<_, anyhow::Error>(serde_json::json!({
            "chat_id": chat,
            "chat_name": name.unwrap_or_else(|| chat.to_string()),
            "media": cards,
            "next_before": next_before,
        }))
    })
    .await;
    match result {
        Ok(Ok(data)) => Ok(Json(data)),
        Ok(Err(err)) => {
            log::warn!("Telegram preview failed: {err:#}");
            Err(error(
                StatusCode::BAD_GATEWAY,
                "Could not load chat media. Check chat access and try again.",
            ))
        }
        Err(_) => Err(error(
            StatusCode::GATEWAY_TIMEOUT,
            "Telegram preview timed out. Try again.",
        )),
    }
}

pub(super) async fn image(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<u64>,
) -> Result<Response, ApiError> {
    let session = session(&state).await?;
    let _permit = tokio::time::timeout(Duration::from_secs(20), session.requests.acquire())
        .await
        .map_err(|_| error(StatusCode::GATEWAY_TIMEOUT, "Thumbnail queue timed out"))?
        .map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Telegram preview is unavailable",
            )
        })?;
    let thumb = session
        .thumbnails
        .lock()
        .await
        .get(&id)
        .cloned()
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "Preview expired. Reload the chat."))?;
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        // Cached/stripped thumbnails are already in the message. Reserve the
        // advertised size before fetching network-backed thumbnails.
        if thumb.to_raw_input_location().is_some() {
            state
                .download_limiter
                .acquire(
                    crate::runtime::download_limiter::DownloadModule::Telegram,
                    thumb.size(),
                )
                .await;
        }
        let mut download = session.client.iter_download(&thumb);
        let mut bytes = Vec::new();
        while let Some(chunk) = download.next().await? {
            anyhow::ensure!(
                bytes.len() + chunk.len() <= MAX_THUMB_BYTES,
                "thumbnail too large"
            );
            bytes.extend_from_slice(&chunk);
        }
        anyhow::ensure!(!bytes.is_empty(), "thumbnail is empty");
        Ok::<_, anyhow::Error>(bytes)
    })
    .await;
    match result {
        Ok(Ok(bytes)) => Ok((
            [
                ("content-type", "image/jpeg"),
                ("cache-control", "private, no-store"),
                ("x-content-type-options", "nosniff"),
            ],
            bytes,
        )
            .into_response()),
        Ok(Err(err)) => {
            log::debug!("Telegram thumbnail unavailable: {err:#}");
            Err(error(StatusCode::BAD_GATEWAY, "Thumbnail unavailable"))
        }
        Err(_) => Err(error(StatusCode::GATEWAY_TIMEOUT, "Thumbnail timed out")),
    }
}

#[derive(Deserialize)]
pub(super) struct DownloadQuery {
    chat_id: String,
    message_id: i32,
}

pub(super) async fn download(
    State(state): State<Arc<ApiState>>,
    Json(payload): Json<DownloadQuery>,
) -> (StatusCode, Json<super::ApiResponse>) {
    let chat = payload.chat_id.trim().trim_start_matches('@');
    let target = if let Ok(id) = chat.parse::<i64>() {
        ChatTarget::DialogId(id)
    } else if super::valid_username(chat) {
        ChatTarget::Username(chat.to_string())
    } else {
        return super::api_response(StatusCode::BAD_REQUEST, "Invalid chat ID");
    };
    if payload.message_id <= 0 {
        return super::api_response(StatusCode::BAD_REQUEST, "Invalid message ID");
    }
    super::queue_target(state, target, Some(payload.message_id), false).await
}

// Keep the weak-session field's type local to this API module.
pub(super) type SessionRef = Weak<Session>;
