CREATE TABLE jav_cookie (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    site TEXT NOT NULL,
    configured_cookie TEXT NOT NULL,
    configured_user_agent TEXT NOT NULL,
    value TEXT NOT NULL,
    user_agent TEXT NOT NULL
);
