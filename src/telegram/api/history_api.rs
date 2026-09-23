//! History edits keep downloaded files and chat scan positions intact.
use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde_json::{Value, json};

use super::{ApiState, now_millis};
use crate::telegram::storage::history;

type Error = (StatusCode, String);

fn storage_error(error: anyhow::Error) -> Error {
    log::warn!("cannot update Telegram history: {error:#}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "Could not update download history".into(),
    )
}

#[derive(serde::Deserialize)]
pub(super) struct ListQuery {
    limit: Option<usize>,
}

pub(super) async fn list(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<ListQuery>,
) -> Json<Value> {
    state.prune_history().await;
    let records = state.history_snapshot(query.limit).await;
    let snapshot = state.snapshot().await;
    Json(json!({
        "records": records,
        "history_retention_days": snapshot.history_retention_days,
        "history_revision": snapshot.history_revision,
        "downloaded_files": snapshot.downloaded_files,
        "downloaded_bytes": snapshot.downloaded_bytes,
    }))
}

pub(super) async fn forget(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, Error> {
    let mut stats = state.stats.lock().await;
    let Some(item) = stats.completed.iter().find(|item| item.key() == id) else {
        return Err((
            StatusCode::NOT_FOUND,
            "History entry no longer exists".into(),
        ));
    };
    let tx = state.database.transaction().await.map_err(storage_error)?;
    history::forget(&tx, item, now_millis())
        .await
        .map_err(storage_error)?;
    tx.commit().await.map_err(storage_error)?;
    stats.completed.retain(|item| item.key() != id);
    drop(stats);
    state.history_changed();
    state.publish().await;
    Ok(Json(json!({ "status": "forgotten", "id": id })))
}

pub(super) async fn clear(State(state): State<Arc<ApiState>>) -> Result<Json<Value>, Error> {
    state.prune_history().await;
    let mut stats = state.stats.lock().await;
    let tx = state.database.transaction().await.map_err(storage_error)?;
    let now = now_millis();
    for item in &stats.completed {
        history::forget(&tx, item, now)
            .await
            .map_err(storage_error)?;
    }
    tx.commit().await.map_err(storage_error)?;
    let removed = stats.completed.len();
    stats.completed.clear();
    drop(stats);
    state.history_changed();
    state.publish().await;
    Ok(Json(json!({ "status": "cleared", "removed": removed })))
}
