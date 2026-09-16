CREATE TABLE legacy_imports (source TEXT PRIMARY KEY);
CREATE TABLE jav_records (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    title TEXT NOT NULL,
    rank INTEGER,
    status TEXT NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL CHECK(size >= 0),
    finished_at TEXT NOT NULL,
    error TEXT
);
CREATE INDEX jav_history_time ON jav_records(finished_at DESC);
CREATE TABLE jav_scheduler (id INTEGER PRIMARY KEY CHECK(id = 1), last_daily_run TEXT);
CREATE TABLE telegram_files (file_id TEXT PRIMARY KEY);
CREATE TABLE telegram_chats (chat_id TEXT PRIMARY KEY, last_read_message_id INTEGER NOT NULL DEFAULT 0);
CREATE TABLE telegram_retries (
    chat_id TEXT NOT NULL REFERENCES telegram_chats(chat_id) ON DELETE CASCADE,
    message_id INTEGER NOT NULL,
    PRIMARY KEY(chat_id, message_id)
);
CREATE TABLE telegram_history (
    id INTEGER PRIMARY KEY,
    message_id INTEGER NOT NULL,
    file_name TEXT NOT NULL,
    path TEXT NOT NULL,
    bytes INTEGER NOT NULL CHECK(bytes >= 0),
    completed_at INTEGER NOT NULL
);
CREATE INDEX telegram_history_time ON telegram_history(completed_at);
CREATE TABLE telegram_progress (path TEXT PRIMARY KEY, downloaded_bytes INTEGER NOT NULL CHECK(downloaded_bytes >= 0));
