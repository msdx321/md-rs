//! HTTP transport and errors shared by engine web interfaces, independent of event schemas.
use std::convert::Infallible;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{
    IntoResponse, Response,
    sse::{Event, KeepAlive, Sse},
};
use futures_util::Stream;
use serde_json::json;

/// A status code with a message, sent to the UI as `{ "error": … }`.
pub struct ApiError(pub StatusCode, pub String);

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, msg.into())
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self(StatusCode::NOT_FOUND, msg.into())
    }
    pub fn internal(msg: impl Into<String>) -> Self {
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

pub type ApiResult<T> = Result<T, ApiError>;

/// Both engines send live updates with keep-alives and proxy buffering disabled.
/// Initial snapshots, event names, and serialization belong to the caller.
pub fn events<S>(stream: S) -> impl IntoResponse
where
    S: Stream<Item = Result<Event, Infallible>> + Send + 'static,
{
    (
        [("x-accel-buffering", "no")],
        Sse::new(stream).keep_alive(KeepAlive::default()),
    )
}
