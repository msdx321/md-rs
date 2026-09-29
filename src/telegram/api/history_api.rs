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
    let history_write = state.history_write.lock().await;
    let item = state
        .stats
        .lock()
        .await
        .completed
        .iter()
        .find(|item| item.key() == id)
        .cloned();
    let Some(item) = item else {
        return Err((
            StatusCode::NOT_FOUND,
            "History entry no longer exists".into(),
        ));
    };
    let tx = state.database.transaction().await.map_err(storage_error)?;
    history::forget(&tx, &item, now_millis())
        .await
        .map_err(storage_error)?;
    tx.commit().await.map_err(storage_error)?;
    state
        .stats
        .lock()
        .await
        .retain_completed(|item| item.key() != id);
    state.history_changed();
    drop(history_write);
    state.publish().await;
    Ok(Json(json!({ "status": "forgotten", "id": id })))
}

pub(super) async fn clear(State(state): State<Arc<ApiState>>) -> Result<Json<Value>, Error> {
    state.prune_history().await;
    // Completion must finish both its database and cache updates before the
    // bulk delete, or wait until both sides have been cleared. Keep snapshots
    // available while the two statements commit.
    let history_write = state.history_write.lock().await;
    let tx = state.database.transaction().await.map_err(storage_error)?;
    // `WHERE true` keeps SQLite from reading `ON CONFLICT` as a join clause.
    tx.execute(
        "INSERT INTO telegram_forgotten(path,forgotten_at) \
         SELECT path,? FROM telegram_history WHERE true \
         ON CONFLICT(path) DO UPDATE SET forgotten_at=excluded.forgotten_at",
        [i64::try_from(now_millis()).unwrap_or(i64::MAX)],
    )
    .await
    .map_err(|error| storage_error(error.into()))?;
    tx.execute("DELETE FROM telegram_history", ())
        .await
        .map_err(|error| storage_error(error.into()))?;
    tx.commit().await.map_err(storage_error)?;
    let removed = state.stats.lock().await.clear_completed();
    state.history_changed();
    drop(history_write);
    state.publish().await;
    Ok(Json(json!({ "status": "cleared", "removed": removed })))
}
