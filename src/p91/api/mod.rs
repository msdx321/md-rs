//! 91Porn commands, settings, and live status API.

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_stream::wrappers::{BroadcastStream, WatchStream};

use crate::p91::app::{AppCtx, TaskInfo, TaskState};
use crate::p91::config::Config;
use crate::p91::scheduler;
use crate::p91::source::scraper::{self, VideoCard};

pub fn router(ctx: Arc<AppCtx>) -> Router {
    Router::new()
        .route("/api/status", get(status))
        .route("/api/access/check", post(check_access))
        .route("/api/config", get(get_config).put(put_config))
        .route("/api/videos", get(list_videos))
        .route("/api/tasks", get(list_tasks))
        .route("/api/tasks/failed", delete(clear_failed_tasks))
        .route("/api/tasks/{id}/pause", post(pause_task))
        .route("/api/tasks/{id}/resume", post(resume_task))
        .route("/api/tasks/{id}/cancel", post(cancel_task))
        .route("/api/tasks/{id}", delete(dismiss_task))
        .route("/api/download", post(start_download))
        .route("/api/daily/run", post(run_daily))
        .route("/api/daily/pause", post(pause_all))
        .route("/api/daily/cancel", post(cancel_all))
        .route("/api/tasks/resume-all", post(resume_all))
        .route("/api/history", get(list_history).delete(clear_history))
        .route("/api/history/{id}", delete(forget_record))
        .route("/api/events", get(events))
        .with_state(ctx)
}

// ── error type ───────────────────────────────────────────────────────────────

pub struct ApiError(StatusCode, String);

impl ApiError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, msg.into())
    }
    fn not_found(msg: impl Into<String>) -> Self {
        Self(StatusCode::NOT_FOUND, msg.into())
    }
    fn internal(msg: impl Into<String>) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, msg.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError::internal(format!("{e:#}"))
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        ApiError::internal(format!("{e}"))
    }
}

type ApiResult<T> = Result<T, ApiError>;

// ── handlers ─────────────────────────────────────────────────────────────────

async fn status(State(ctx): State<Arc<AppCtx>>) -> Json<Value> {
    Json(status_snapshot(&ctx))
}

fn status_snapshot(ctx: &AppCtx) -> Value {
    let cfg = ctx.config();
    let history = ctx.history_summary();
    let tasks = ctx.tasks();
    let active = tasks
        .iter()
        .filter(|t| t.state == TaskState::Running)
        .count();
    json!({
        "access_configured": cfg.request_cookie().is_some(),
        "session_ready": ctx.session().is_warm(),
        "prefer_hd": cfg.prefer_hd,
        "site": cfg.site_base,
        "save_path": cfg.save_path,
        "top_n": cfg.listing_links().iter().map(|link| link.daily_quota).sum::<usize>(),
        "links": cfg.listing_links(),
        "history_retention_days": ctx.history_retention_days(),
        "downloaded": history.completed,
        "failed_records": history.failed,
        "downloaded_bytes": history.total_bytes,
        "active_tasks": active,
        "last_daily_run": history.last_daily_run,
        "scheduler": ctx.scheduler_status(),
        "version": env!("CARGO_PKG_VERSION"),
    })
}

/// Fetch the first listing page to confirm the configured site and cookie work.
async fn check_access(State(ctx): State<Arc<AppCtx>>) -> ApiResult<Json<Value>> {
    let cfg = ctx.config();
    let fetch = ctx.fetcher();
    let cards = scraper::fetch_popular(&fetch, &cfg.for_link(&cfg.listing_links()[0]), 1)
        .await
        .map_err(|e| ApiError::internal(format!("{e:#}")))?;
    ctx.session().mark_warm();
    Ok(Json(json!({
        "ok": true,
        "videos": cards.len(),
        "access_configured": cfg.request_cookie().is_some(),
    })))
}

async fn get_config(State(ctx): State<Arc<AppCtx>>) -> Json<Config> {
    Json(ctx.config())
}

async fn put_config(
    State(ctx): State<Arc<AppCtx>>,
    Json(patch): Json<Value>,
) -> ApiResult<Json<Config>> {
    let _update = ctx.config_update.lock().await;
    let current = serde_json::to_value(ctx.config())?;
    let mut merged = current;
    merge_json(&mut merged, patch);
    let cfg: Config = serde_json::from_value(merged)
        .map_err(|e| ApiError::bad_request(format!("invalid configuration: {e}")))?;

    if !(1..=100).contains(&cfg.top_n) {
        return Err(ApiError::bad_request("top_n must be 1..=100"));
    }
    if !(1..=20).contains(&cfg.max_pages) {
        return Err(ApiError::bad_request("max_pages must be 1..=20"));
    }
    if !cfg.min_duration_secs.is_finite() || cfg.min_duration_secs < 0.0 {
        return Err(ApiError::bad_request(
            "min_duration_secs must be nonnegative",
        ));
    }
    if cfg.concurrent_videos == 0 || cfg.concurrent_videos > 8 {
        return Err(ApiError::bad_request("concurrent_videos must be 1..=8"));
    }

    cfg.title_matcher()
        .map_err(|e| ApiError::bad_request(format!("{e:#}")))?;
    cfg.validate_links()
        .map_err(|e| ApiError::bad_request(format!("{e:#}")))?;
    ctx.update_config(cfg.clone()).await?;
    log::info!("91Porn configuration updated via the web UI");
    Ok(Json(cfg))
}

/// Recursively overlay `patch` onto `base`; objects merge, everything else
/// replaces. `null` removes an optional value.
fn merge_json(base: &mut Value, patch: Value) {
    match (base, patch) {
        (Value::Object(base), Value::Object(patch)) => {
            for (key, value) in patch {
                merge_json(base.entry(key).or_insert(Value::Null), value);
            }
        }
        (slot, value) => *slot = value,
    }
}

#[derive(Debug, Deserialize)]
struct VideosQuery {
    #[serde(default = "default_page")]
    page: usize,
    #[serde(default)]
    link: usize,
}

fn default_page() -> usize {
    1
}

#[derive(Debug, Serialize)]
struct VideoView {
    #[serde(flatten)]
    card: VideoCard,
    downloaded: bool,
    in_progress: bool,
}

async fn list_videos(
    State(ctx): State<Arc<AppCtx>>,
    Query(query): Query<VideosQuery>,
) -> ApiResult<Json<Value>> {
    let cfg = ctx.config();
    let links = cfg.listing_links();
    let link = links
        .get(query.link)
        .ok_or_else(|| ApiError::bad_request("unknown listing link"))?;
    let cfg = cfg.for_link(link);
    let fetch = ctx.fetcher();
    let page = query.page.max(1);
    let cards = scraper::fetch_popular(&fetch, &cfg, page)
        .await
        .map_err(|e| ApiError::internal(format!("{e:#}")))?;
    // The listing request is what establishes the site session.
    ctx.session().mark_warm();

    let videos: Vec<VideoView> = cards
        .into_iter()
        .map(|card| {
            let downloaded = ctx.is_completed(&card.id);
            let in_progress = ctx
                .task(&card.id)
                .map(|t| !t.is_terminal())
                .unwrap_or(false);
            VideoView {
                card,
                downloaded,
                in_progress,
            }
        })
        .collect();

    Ok(Json(json!({
        "page": page,
        "link": query.link,
        "videos": videos,
        "title_filter_active": !cfg.title_filter.trim().is_empty(),
    })))
}

async fn list_tasks(State(ctx): State<Arc<AppCtx>>) -> Json<Vec<TaskInfo>> {
    Json(
        ctx.tasks()
            .into_iter()
            .filter(|t| t.state != TaskState::Completed)
            .collect(),
    )
}

#[derive(Debug, Deserialize)]
struct DownloadRequest {
    id: Option<String>,
    url: Option<String>,
    title: Option<String>,
    rank: Option<usize>,
    /// Internal video id from the listing card, used to detect decoy pages.
    vid: Option<String>,
}

async fn start_download(
    State(ctx): State<Arc<AppCtx>>,
    Json(req): Json<DownloadRequest>,
) -> ApiResult<Json<Value>> {
    let (id, url) = match (&req.id, &req.url) {
        (Some(id), Some(url)) => (id.clone(), url.clone()),
        (Some(id), None) => (id.clone(), ctx.config().video_url(id)),
        (None, Some(url)) => {
            let id = scraper::video_id_from_url(url).ok_or_else(|| {
                ApiError::bad_request(
                    "url must look like https://www.91porn.com/view_video.php?viewkey=…",
                )
            })?;
            (id, url.clone())
        }
        (None, None) => return Err(ApiError::bad_request("id or url is required")),
    };

    // The id is a view key, and it has to agree with the URL so a mismatch
    // cannot download the wrong video.
    if id.is_empty() || scraper::video_id_from_url(&url).as_deref() != Some(id.as_str()) {
        return Err(ApiError::bad_request(
            "id must be a view key that matches the video URL",
        ));
    }

    if ctx.is_completed(&id) {
        return Ok(Json(json!({
            "status": "already_downloaded",
            "id": id,
        })));
    }

    let card = VideoCard {
        id: id.clone(),
        url,
        title: req.title.unwrap_or_default(),
        image_url: String::new(),
        duration_secs: None,
        rank: req.rank,
        vid: req.vid,
        hd: false,
        original: false,
    };

    let mut info = TaskInfo::new(&card.id, &card.url);
    info.title = card.title.clone();
    info.vid = card.vid.clone();
    if !ctx.register_task(info) {
        return Ok(Json(json!({ "status": "already_running", "id": id })));
    }

    let ctx_for_task = Arc::clone(&ctx);
    tokio::spawn(async move {
        crate::p91::downloader::download_video(ctx_for_task, card).await;
    });

    Ok(Json(json!({ "status": "started", "id": id })))
}

async fn pause_task(
    State(ctx): State<Arc<AppCtx>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let task = ctx
        .task(&id)
        .ok_or_else(|| ApiError::not_found("no such task"))?;
    if !matches!(task.state, TaskState::Running | TaskState::Queued) {
        return Err(ApiError::bad_request("task is not running or queued"));
    }
    ctx.update_task(&id, |t| {
        if t.state == TaskState::Queued {
            t.state = TaskState::Paused;
            t.phase = "paused".into();
            t.message = "paused by user".into();
        } else if t.state == TaskState::Running {
            t.phase = "pausing".into();
            t.message = "pausing…".into();
        }
    });
    Ok(Json(json!({ "status": "pausing", "id": id })))
}

async fn resume_task(
    State(ctx): State<Arc<AppCtx>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    if !scheduler::resume_task(Arc::clone(&ctx), &id) {
        return Err(ApiError::bad_request("task cannot be resumed"));
    }
    Ok(Json(json!({ "status": "resumed", "id": id })))
}

async fn cancel_task(
    State(ctx): State<Arc<AppCtx>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let task = ctx
        .task(&id)
        .ok_or_else(|| ApiError::not_found("no such task"))?;
    if task.is_terminal() {
        return Err(ApiError::bad_request("task is already finished"));
    }
    ctx.update_task(&id, |t| {
        t.state = TaskState::Cancelled;
        if task.state == TaskState::Queued {
            t.phase = "cancelled".into();
            t.message = "cancelled by user".into();
        } else {
            t.phase = "cancelling".into();
            t.message = "cancelling…".into();
        }
    });
    Ok(Json(json!({ "status": "cancelling", "id": id })))
}

async fn clear_failed_tasks(State(ctx): State<Arc<AppCtx>>) -> Json<Value> {
    Json(json!({ "count": ctx.clear_failed_tasks() }))
}

async fn dismiss_task(
    State(ctx): State<Arc<AppCtx>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    match ctx.task(&id) {
        Some(task) if !task.is_terminal() => Err(ApiError::bad_request(
            "cancel the task before dismissing it",
        )),
        Some(_) => {
            ctx.drop_task(&id);
            Ok(Json(json!({ "status": "dismissed", "id": id })))
        }
        None => Err(ApiError::not_found("no such task")),
    }
}

async fn run_daily(State(ctx): State<Arc<AppCtx>>) -> Json<Value> {
    let ctx_for_job = Arc::clone(&ctx);
    tokio::spawn(async move {
        if let Err(e) = scheduler::run_daily(ctx_for_job, "manual").await {
            log::error!("manual daily run failed: {e:#}");
        }
    });
    Json(json!({ "status": "started" }))
}

async fn pause_all(State(ctx): State<Arc<AppCtx>>) -> Json<Value> {
    scheduler::pause_active(&ctx);
    Json(json!({ "status": "paused_all" }))
}

async fn cancel_all(State(ctx): State<Arc<AppCtx>>) -> Json<Value> {
    scheduler::cancel_active(&ctx);
    Json(json!({ "status": "cancelled_all" }))
}

async fn resume_all(State(ctx): State<Arc<AppCtx>>) -> Json<Value> {
    let count = scheduler::resume_all(ctx).await;
    Json(json!({ "status": "resumed", "count": count }))
}

async fn list_history(State(ctx): State<Arc<AppCtx>>) -> Json<Value> {
    let records = ctx.history();
    Json(json!({
        "history_retention_days": ctx.history_retention_days(),
        "count": records.len(),
        "records": records,
    }))
}

async fn forget_record(
    State(ctx): State<Arc<AppCtx>>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    ctx.forget_record(&id).await?;
    Ok(Json(json!({ "status": "forgotten", "id": id })))
}

async fn clear_history(State(ctx): State<Arc<AppCtx>>) -> ApiResult<Json<Value>> {
    let removed = ctx.clear_history().await?;
    Ok(Json(json!({ "status": "cleared", "removed": removed })))
}

async fn events(State(ctx): State<Arc<AppCtx>>) -> impl IntoResponse {
    let rx = ctx.subscribe();
    let tasks = BroadcastStream::new(rx).filter_map(|result| async move {
        match result {
            Ok(task) => match serde_json::to_string(&task) {
                Ok(data) => Some(Ok::<_, Infallible>(
                    Event::default().event("task").data(data),
                )),
                Err(_) => None,
            },
            // A lagged subscriber simply misses intermediate frames.
            Err(_) => None,
        }
    });
    // WatchStream emits its current value immediately, flushing the SSE body
    // even when no tasks are running.
    let statuses = WatchStream::new(ctx.subscribe_status()).map(move |()| {
        Ok::<_, Infallible>(
            Event::default()
                .event("status")
                .data(status_snapshot(&ctx).to_string()),
        )
    });
    crate::runtime::web::events(futures_util::stream::select(tasks, statuses))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_overlays_nested_objects() {
        let mut base = json!({"a": 1, "nested": {"x": 1, "y": 2}});
        merge_json(&mut base, json!({"nested": {"y": 9}, "b": 2}));
        assert_eq!(base, json!({"a": 1, "b": 2, "nested": {"x": 1, "y": 9}}));
    }

    #[test]
    fn merge_allows_clearing_optionals_with_null() {
        let mut base = json!({"cookie": "x"});
        merge_json(&mut base, json!({"cookie": null}));
        assert_eq!(base, json!({"cookie": null}));
    }
}
