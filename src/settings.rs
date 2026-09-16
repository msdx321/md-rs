use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
use media_config::app::Config;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

type Settings = Arc<Mutex<watch::Sender<Config>>>;

pub(crate) fn router(sender: watch::Sender<Config>) -> Router {
    Router::new()
        .route("/api/config", get(read).put(save))
        .with_state(Arc::new(Mutex::new(sender)))
}

async fn read(State(state): State<Settings>) -> Json<Config> {
    Json(state.lock().await.borrow().clone())
}

async fn save(
    State(state): State<Settings>,
    Json(config): Json<Config>,
) -> Result<Json<Config>, (StatusCode, String)> {
    config
        .validate()
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let state = state.lock().await;
    media_config::app::FILE
        .save(&config)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    state.send_replace(config.clone());
    Ok(Json(config))
}
