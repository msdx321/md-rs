//! Keep recent application logs available to the UI as well as the terminal.
use std::{
    collections::VecDeque,
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use axum::{
    Json,
    extract::{MatchedPath, Query, Request},
    http::header,
    middleware::Next,
    response::{IntoResponse, Response},
};
use log::{Log, Metadata, Record};
use serde::{Deserialize, Serialize};
use serde_json::json;

const CAPACITY: usize = 1_000;
static ENTRIES: Mutex<VecDeque<Entry>> = Mutex::new(VecDeque::new());

#[derive(Clone, Serialize)]
struct Entry {
    id: u64,
    timestamp: String,
    level: String,
    target: String,
    message: String,
}

struct Logger(env_logger::Logger);

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.0.enabled(metadata)
    }

    fn log(&self, record: &Record<'_>) {
        if !self.0.matches(record) {
            return;
        }
        self.0.log(record);
        let message = record.args().to_string();
        let mut entries = ENTRIES.lock().expect("log buffer lock poisoned");
        let id = entries.back().map_or(1, |entry| entry.id + 1);
        if entries.len() == CAPACITY {
            entries.pop_front();
        }
        entries.push_back(Entry {
            id,
            timestamp: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            level: record.level().to_string(),
            target: record.target().into(),
            message,
        });
    }

    fn flush(&self) {
        self.0.flush();
    }
}

pub(crate) fn init() {
    // Keep dependency chatter (including Chromium's listener cleanup) out of
    // normal logs. Parse RUST_LOG as the complete filter so explicit dependency
    // debug/trace directives are never silently overridden.
    let logger = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("warn,md_rs=info"),
    )
    .format_timestamp_millis()
    .build();
    let filter = logger.filter();
    log::set_boxed_logger(Box::new(Logger(logger))).expect("logger already initialized");
    log::set_max_level(filter);
}

/// Log response headers, not completion of streaming bodies (SSE can stay open).
/// Use route templates only: URLs, queries, headers and bodies may hold secrets.
pub(crate) async fn request_log(request: Request, next: Next) -> Response {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("<unmatched>", MatchedPath::as_str)
        .to_owned();
    let method = request.method().clone();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    // Viewing logs should not generate more successful log entries itself.
    let log_success = route != "/api/logs";
    if log_success {
        log::trace!(target: "md_rs::http", "request={id} method={method} route={route} started");
    }
    let response = next.run(request).await;
    let status = response.status();
    let level = if status.is_server_error() {
        log::Level::Error
    } else if status.is_client_error() {
        log::Level::Warn
    } else {
        log::Level::Debug
    };
    if log_success || status.is_client_error() || status.is_server_error() {
        log::log!(
            target: "md_rs::http",
            level,
            "request={id} method={method} route={route} status={} elapsed_ms={:.3}",
            status.as_u16(),
            started.elapsed().as_secs_f64() * 1000.0,
        );
    }
    response
}

#[derive(Default, Deserialize)]
pub(crate) struct LogQuery {
    after_id: Option<u64>,
    session: Option<String>,
}

pub(crate) async fn snapshot(Query(query): Query<LogQuery>) -> impl IntoResponse {
    static SESSION: LazyLock<String> = LazyLock::new(|| {
        format!(
            "{}-{}",
            std::process::id(),
            chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
        )
    });
    let entries = ENTRIES.lock().expect("log buffer lock poisoned");
    let cursor = entries.back().map_or(0, |entry| entry.id);
    let oldest_id = entries.front().map_or(1, |entry| entry.id);
    let reset = query.session.as_deref() != Some(SESSION.as_str())
        || query
            .after_id
            .is_none_or(|id| id > cursor || id < oldest_id.saturating_sub(1));
    let rows: Vec<_> = entries
        .iter()
        .filter(|entry| reset || query.after_id.is_none_or(|id| entry.id > id))
        .cloned()
        .collect();
    drop(entries);
    // Preserve the original full-array API for clients without a cursor.
    let body = if query.after_id.is_none() {
        json!(rows)
    } else {
        json!({
            "session": *SESSION, "reset": reset, "cursor": cursor,
            "oldest_id": oldest_id, "entries": rows,
        })
    };
    ([(header::CACHE_CONTROL, "no-store")], Json(body))
}
