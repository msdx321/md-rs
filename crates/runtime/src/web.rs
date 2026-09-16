//! HTTP transport shared by engine web interfaces, independent of event schemas.
use std::convert::Infallible;

use axum::response::{
    IntoResponse,
    sse::{Event, KeepAlive, Sse},
};
use futures_util::Stream;

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
