CREATE TABLE telegram_cursor_overrides (
    chat_id TEXT PRIMARY KEY,
    last_read_message_id INTEGER NOT NULL CHECK(last_read_message_id >= 0)
);
