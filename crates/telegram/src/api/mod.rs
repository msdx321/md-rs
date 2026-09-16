//! Telegram commands, login, and live status API.
use crate::storage::history;
use tokio_util::sync::CancellationToken;
mod login;
mod settings;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::sse::Event;
use axum::routing::{get, post};
use axum::{Json, Router, extract::State};
use grammers_session::types::PeerId;
use log::warn;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast, mpsc, watch};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamExt, once};

use crate::format::format_byte;
use history::CompletedDownload;

const PROGRESS_UPDATE_MS: u64 = 2000;
const MAX_CHAT_LINK_LEN: usize = 512;

#[derive(Debug)]
pub enum ChatTarget {
    Username(String),
    DialogId(i64),
    Invite(String),
}

#[derive(Debug)]
pub enum ChatRequest {
    Once(ChatTarget, i32),
    Subscribe(ChatTarget),
}

impl ChatTarget {
    fn parse(input: &str) -> Result<(Self, Option<i32>), &'static str> {
        let input = input.trim();
        if input.is_empty() || input.len() > MAX_CHAT_LINK_LEN {
            return Err("Enter a Telegram chat link");
        }
        if let Some(username) = input.strip_prefix('@') {
            return valid_username(username)
                .then(|| (Self::Username(username.to_string()), None))
                .ok_or("Invalid Telegram username");
        }

        let (normalized, rest) = match input.split_once("://") {
            Some((scheme, rest))
                if matches!(scheme.to_ascii_lowercase().as_str(), "http" | "https") =>
            {
                (input.to_string(), rest)
            }
            Some(_) => return Err("Use an http or https Telegram link"),
            None => (format!("https://{input}"), input),
        };
        let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
        let host = host.to_ascii_lowercase();
        if !matches!(
            host.as_str(),
            "t.me" | "telegram.me" | "telegram.dog" | "tg.dev" | "telesco.pe"
        ) {
            return Err("Use a Telegram chat link");
        }

        let path = path.split(['?', '#']).next().unwrap_or_default();
        let parts: Vec<_> = path.split('/').filter(|part| !part.is_empty()).collect();
        match parts.as_slice() {
            [first, ..] if first.starts_with('+') && valid_invite_hash(&first[1..]) => {
                Ok((Self::Invite(normalized), None))
            }
            ["joinchat", hash, ..] if valid_invite_hash(hash) => {
                Ok((Self::Invite(normalized), None))
            }
            ["c", id, rest @ ..] => id
                .parse::<i64>()
                .ok()
                .and_then(PeerId::channel)
                .and_then(PeerId::bot_api_dialog_id)
                .map(|id| (Self::DialogId(id), message_id(rest)))
                .ok_or("Invalid private Telegram chat link"),
            ["s", username, rest @ ..] if valid_username(username) => {
                Ok((Self::Username((*username).to_string()), message_id(rest)))
            }
            [username, rest @ ..] if valid_username(username) => {
                Ok((Self::Username((*username).to_string()), message_id(rest)))
            }
            _ => Err("Invalid Telegram chat link"),
        }
    }

    pub fn label(&self) -> String {
        match self {
            Self::Username(username) => format!("@{username}"),
            Self::DialogId(id) => format!("chat {id}"),
            Self::Invite(_) => "invite link".to_string(),
        }
    }
}

fn message_id(parts: &[&str]) -> Option<i32> {
    parts.last()?.parse().ok().filter(|id| *id > 0)
}

fn valid_username(username: &str) -> bool {
    !username.is_empty()
        && username.len() <= 32
        && username
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn valid_invite_hash(hash: &str) -> bool {
    !hash.is_empty()
        && hash.len() <= 128
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub struct ApiState {
    pub(crate) download_cancel: Mutex<CancellationToken>,
    pub(crate) cancelling: AtomicBool,
    pub(crate) config_update: Mutex<()>,
    pub(crate) settings_changed: watch::Sender<()>,
    pub(crate) database: media_storage::Database,
    login: Mutex<login::Login>,
    download_tx: mpsc::Sender<ChatRequest>,
    paused: AtomicBool,
    last_progress_publish_ms: AtomicU64,
    pause_tx: watch::Sender<bool>,
    request_status: Mutex<String>,
    status: Mutex<String>,
    stats: Mutex<DashboardStats>,
    updates: broadcast::Sender<String>,
}

#[derive(Default)]
struct DashboardStats {
    completed: Vec<CompletedDownload>,
    active: BTreeMap<i32, DownloadStat>,
}

struct DownloadStat {
    file_name: String,
    path: String,
    downloaded: u64,
    total: u64,
    speed_bps: u64,
}

#[derive(Serialize)]
struct DashboardSnapshot {
    login: login::LoginSnapshot,
    status: String,
    request_status: String,
    paused: bool,
    cancelling: bool,
    downloaded_files: u64,
    downloaded_bytes: String,
    active_count: usize,
    active: Vec<DownloadSnapshot>,
    completed: Vec<CompletedSnapshot>,
}

#[derive(Serialize)]
struct CompletedSnapshot {
    msg_id: i32,
    file_name: String,
    path: String,
    size: String,
    completed_at: u64,
}

#[derive(Serialize)]
struct DownloadSnapshot {
    msg_id: i32,
    file_name: String,
    path: String,
    downloaded: String,
    total: String,
    speed: String,
    percent: f64,
}

impl ApiState {
    pub async fn new(
        download_tx: mpsc::Sender<ChatRequest>,
        database: media_storage::Database,
    ) -> anyhow::Result<Self> {
        let (updates, _) = broadcast::channel(64);
        let (pause_tx, _) = watch::channel(false);
        Ok(Self {
            login: login::new(),
            download_cancel: Mutex::new(CancellationToken::new()),
            cancelling: AtomicBool::new(false),
            config_update: Mutex::new(()),
            settings_changed: watch::channel(()).0,
            download_tx,
            paused: AtomicBool::new(false),
            last_progress_publish_ms: AtomicU64::new(0),
            pause_tx,
            request_status: Mutex::new(
                "Paste a Telegram message or chat link to begin".to_string(),
            ),
            status: Mutex::new("starting".to_string()),
            stats: Mutex::new(DashboardStats {
                completed: history::load(&database, now_millis()).await?,
                ..DashboardStats::default()
            }),
            updates,
            database,
        })
    }

    pub async fn set_status(&self, status: &str) {
        *self.status.lock().await = status.to_string();
        self.publish().await;
    }

    pub async fn set_request_status(&self, status: &str) {
        *self.request_status.lock().await = status.to_string();
        self.publish().await;
    }

    pub async fn wait_if_paused(&self) {
        if !self.paused.load(Ordering::Relaxed) {
            return;
        }
        let mut pause_rx = self.pause_tx.subscribe();
        while self.paused.load(Ordering::Relaxed) {
            if pause_rx.changed().await.is_err() {
                break;
            }
        }
    }

    pub async fn download_started(&self, msg_id: i32, path: &Path, downloaded: u64, total: u64) {
        let mut stats = self.stats.lock().await;
        stats.active.insert(
            msg_id,
            DownloadStat {
                file_name: path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| path.display().to_string()),
                path: path.display().to_string(),
                downloaded,
                total,
                speed_bps: 0,
            },
        );
        drop(stats);
        self.publish().await;
    }

    pub async fn download_progress(&self, msg_id: i32, downloaded: u64, speed_bps: u64) {
        if let Some(item) = self.stats.lock().await.active.get_mut(&msg_id) {
            item.downloaded = downloaded;
            item.speed_bps = speed_bps;
        }
        self.publish_progress().await;
    }

    pub async fn download_finished(&self, msg_id: i32, bytes: u64, completed: bool) {
        let mut stats = self.stats.lock().await;
        if let Some(item) = stats.active.remove(&msg_id)
            && completed
        {
            let now = now_millis();
            let completed = CompletedDownload {
                msg_id,
                file_name: item.file_name,
                path: item.path,
                bytes,
                completed_at: now,
            };
            let conn = self.database.connection().await;
            match history::insert(&conn, &completed).await {
                Ok(()) => stats.completed.push(completed),
                Err(error) => warn!("cannot save download history: {error:#}"),
            }
        }
        drop(stats);
        self.publish().await;
    }

    pub(crate) async fn prune_history(&self) {
        let mut stats = self.stats.lock().await;
        let now = now_millis();
        let cutoff = now.saturating_sub(history::RETENTION_MS);
        if stats
            .completed
            .iter()
            .any(|item| item.completed_at <= cutoff)
        {
            match history::delete_expired(&*self.database.connection().await, now).await {
                Ok(()) => stats.completed.retain(|item| item.completed_at > cutoff),
                Err(error) => warn!("cannot prune download history: {error:#}"),
            }
        }
    }

    async fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Relaxed);
        let _ = self.pause_tx.send(paused);
        self.set_status(if paused { "paused" } else { "running" })
            .await;
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.updates.subscribe()
    }

    async fn publish_progress(&self) {
        let now = now_millis();
        let mut last = self.last_progress_publish_ms.load(Ordering::Relaxed);
        loop {
            if now.saturating_sub(last) < PROGRESS_UPDATE_MS {
                return;
            }
            match self.last_progress_publish_ms.compare_exchange_weak(
                last,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => last = observed,
            }
        }
        self.publish().await;
    }

    pub(crate) async fn publish(&self) {
        if self.updates.receiver_count() == 0 {
            return;
        }
        let _ = self.updates.send(self.snapshot_json().await);
    }

    async fn snapshot_json(&self) -> String {
        serde_json::to_string(&self.snapshot().await).unwrap_or_else(|_| "{}".to_string())
    }

    async fn snapshot(&self) -> DashboardSnapshot {
        self.prune_history().await;
        let status = self.status.lock().await.clone();
        let request_status = self.request_status.lock().await.clone();
        let stats = self.stats.lock().await;
        let active = stats
            .active
            .iter()
            .map(|(msg_id, item)| {
                let percent = if item.total == 0 {
                    0.0
                } else {
                    item.downloaded as f64 * 100.0 / item.total as f64
                };
                DownloadSnapshot {
                    msg_id: *msg_id,
                    file_name: item.file_name.clone(),
                    path: item.path.clone(),
                    downloaded: format_byte(item.downloaded as f64),
                    total: format_byte(item.total as f64),
                    speed: format!("{}/s", format_byte(item.speed_bps as f64)),
                    percent,
                }
            })
            .collect();

        let completed = stats
            .completed
            .iter()
            .rev()
            .map(|item| CompletedSnapshot {
                msg_id: item.msg_id,
                file_name: item.file_name.clone(),
                path: item.path.clone(),
                size: format_byte(item.bytes as f64),
                completed_at: item.completed_at,
            })
            .collect();

        DashboardSnapshot {
            login: self.login.lock().await.snapshot.clone(),
            status,
            request_status,
            paused: self.paused.load(Ordering::Relaxed),
            cancelling: self.cancelling.load(Ordering::Relaxed),
            downloaded_files: stats.completed.len() as u64,
            downloaded_bytes: format_byte(
                stats.completed.iter().map(|item| item.bytes as f64).sum(),
            ),
            active_count: stats.active.len(),
            active,
            completed,
        }
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/events", get(events))
        .route("/api/config", get(settings::get).put(settings::put))
        .route("/login", post(login::submit))
        .route("/downloads", post(download))
        .route("/subscriptions", post(subscribe))
        .route("/pause", post(pause))
        .route("/resume", post(resume))
        .route("/cancel", post(cancel))
        .with_state(state)
}

#[derive(Deserialize)]
struct DownloadPayload {
    chat_link: String,
}

#[derive(Serialize)]
struct ApiResponse {
    message: String,
}

async fn download(
    State(state): State<Arc<ApiState>>,
    Json(payload): Json<DownloadPayload>,
) -> (StatusCode, Json<ApiResponse>) {
    queue_request(state, payload, false).await
}

async fn subscribe(
    State(state): State<Arc<ApiState>>,
    Json(payload): Json<DownloadPayload>,
) -> (StatusCode, Json<ApiResponse>) {
    queue_request(state, payload, true).await
}

async fn queue_request(
    state: Arc<ApiState>,
    payload: DownloadPayload,
    subscribe: bool,
) -> (StatusCode, Json<ApiResponse>) {
    if state.login.lock().await.snapshot.step != "ready" {
        return api_response(
            StatusCode::CONFLICT,
            "Connect Telegram before adding downloads",
        );
    }
    let (target, message_id) = match ChatTarget::parse(&payload.chat_link) {
        Ok(parsed) => parsed,
        Err(message) => return api_response(StatusCode::BAD_REQUEST, message),
    };
    let label = target.label();
    let (request, message) = if subscribe {
        (
            ChatRequest::Subscribe(target),
            format!("Queued {label} for subscription"),
        )
    } else {
        let Some(message_id) = message_id else {
            return api_response(
                StatusCode::BAD_REQUEST,
                "Download once requires a Telegram message link",
            );
        };
        (
            ChatRequest::Once(target, message_id),
            format!("Queued message {message_id} from {label} for one-shot download"),
        )
    };
    let cancellation = state.download_cancel.lock().await;
    if cancellation.is_cancelled() {
        return api_response(
            StatusCode::CONFLICT,
            "Resume downloads before adding another request",
        );
    }
    match state.download_tx.try_send(request) {
        Ok(()) => {
            state.set_request_status(&message).await;
            api_response(StatusCode::ACCEPTED, &message)
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            api_response(StatusCode::TOO_MANY_REQUESTS, "The download queue is full")
        }
        Err(_) => api_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "The downloader is not ready",
        ),
    }
}

fn api_response(status: StatusCode, message: &str) -> (StatusCode, Json<ApiResponse>) {
    (
        status,
        Json(ApiResponse {
            message: message.to_string(),
        }),
    )
}

async fn pause(State(state): State<Arc<ApiState>>) {
    state.set_paused(true).await;
}

async fn resume(State(state): State<Arc<ApiState>>) -> Result<(), (StatusCode, &'static str)> {
    let mut cancellation = state.download_cancel.lock().await;
    if state.cancelling.load(Ordering::Relaxed) {
        return Err((
            StatusCode::CONFLICT,
            "Wait for current downloads to stop before resuming",
        ));
    }
    if cancellation.is_cancelled() {
        *cancellation = CancellationToken::new();
    }
    state.set_paused(false).await;
    Ok(())
}

async fn cancel(State(state): State<Arc<ApiState>>) -> Result<(), (StatusCode, &'static str)> {
    if state.login.lock().await.snapshot.step != "ready" {
        return Err((
            StatusCode::CONFLICT,
            "Connect Telegram before cancelling downloads",
        ));
    }
    let cancellation = state.download_cancel.lock().await;
    if cancellation.is_cancelled() {
        return Ok(());
    }
    state.cancelling.store(true, Ordering::Relaxed);
    cancellation.cancel();
    state.set_paused(true).await;
    state.set_status("Cancelling downloads…").await;
    Ok(())
}

async fn events(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let receiver = state.subscribe();
    let initial = once(Ok(Event::default().data(state.snapshot_json().await)));
    let updates = BroadcastStream::new(receiver)
        .filter_map(Result::ok)
        .map(|snapshot| Ok(Event::default().data(snapshot)));

    media_runtime::web::events(initial.chain(updates))
}
