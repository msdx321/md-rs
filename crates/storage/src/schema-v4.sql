ALTER TABLE telegram_files ADD COLUMN downloaded_at INTEGER NOT NULL DEFAULT 0;
UPDATE telegram_files SET downloaded_at = unixepoch() * 1000;
CREATE INDEX telegram_files_time ON telegram_files(downloaded_at);
