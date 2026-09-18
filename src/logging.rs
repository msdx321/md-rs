//! Keep recent application logs available to the UI as well as the terminal.
use std::{collections::VecDeque, sync::Mutex};

use axum::{Json, http::header, response::IntoResponse};
use log::{Log, Metadata, Record};
use serde::Serialize;

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
    let logger =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .filter_module("grammers_mtsender", log::LevelFilter::Warn)
            .filter_module("grammers_mtproto", log::LevelFilter::Warn)
            .filter_module("turso_core", log::LevelFilter::Warn)
            .build();
    let filter = logger.filter();
    log::set_boxed_logger(Box::new(Logger(logger))).expect("logger already initialized");
    log::set_max_level(filter);
}

pub(crate) async fn snapshot() -> impl IntoResponse {
    let entries = ENTRIES.lock().expect("log buffer lock poisoned").clone();
    ([(header::CACHE_CONTROL, "no-store")], Json(entries))
}
