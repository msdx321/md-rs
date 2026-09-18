CREATE TABLE p91_records (
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
CREATE INDEX p91_history_time ON p91_records(finished_at DESC);
CREATE TABLE p91_scheduler (id INTEGER PRIMARY KEY CHECK(id = 1), last_daily_run TEXT);
