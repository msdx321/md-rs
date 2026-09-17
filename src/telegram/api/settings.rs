//! Editable download preferences. Credentials are kept private.
use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};

use super::{ApiState, valid_username};
use crate::telegram::config::{ChatConfig, Config, FILE, FileFormats};

type Error = (StatusCode, String);

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct ChatSettings {
    #[serde(flatten)]
    config: ChatConfig,
    #[serde(default)]
    last_read_message_id: Option<i32>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Settings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    chat: Option<Vec<ChatSettings>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    original_chat: Option<Vec<ChatSettings>>,
    max_download_task: usize,
    download_connections: usize,
    media_types: Vec<String>,
    file_formats: FileFormats,
    file_path_prefix: Vec<String>,
    file_name_prefix: Vec<String>,
    file_name_prefix_split: String,
    date_format: String,
}

impl From<Config> for Settings {
    fn from(cfg: Config) -> Self {
        Self {
            chat: Some(
                cfg.chat
                    .into_iter()
                    .map(|config| ChatSettings {
                        config,
                        last_read_message_id: None,
                    })
                    .collect(),
            ),
            original_chat: None,
            max_download_task: cfg.max_download_task,
            download_connections: cfg.download_connections,
            media_types: cfg.media_types,
            file_formats: cfg.file_formats,
            file_path_prefix: cfg.file_path_prefix,
            file_name_prefix: cfg.file_name_prefix,
            file_name_prefix_split: cfg.file_name_prefix_split,
            date_format: cfg.date_format,
        }
    }
}

impl Settings {
    fn validate(&mut self) -> Result<(), Error> {
        let invalid = |message: &str| (StatusCode::BAD_REQUEST, message.to_owned());
        if let Some(chats) = &mut self.chat {
            let mut seen = std::collections::HashSet::new();
            for entry in chats {
                if entry.last_read_message_id.is_some_and(|id| id < 0) {
                    return Err(invalid(
                        "Last read message ID must be zero or a positive integer",
                    ));
                }
                let chat = &mut entry.config;
                let id = chat.chat_id.trim().trim_start_matches('@');
                chat.chat_id = if let Ok(id) = id.parse::<i64>() {
                    if id == 0 {
                        return Err(invalid("Chat ID cannot be zero"));
                    }
                    id.to_string()
                } else if valid_username(id) {
                    id.to_ascii_lowercase()
                } else {
                    return Err(invalid("Use a chat username or numeric Telegram chat ID"));
                };
                if !seen.insert(chat.chat_id.clone()) {
                    return Err(invalid("Each subscribed chat must be listed only once"));
                }
                chat.download_filter = chat
                    .download_filter
                    .take()
                    .map(|filter| filter.trim().to_owned())
                    .filter(|filter| !filter.is_empty());
            }
        }
        if !(1..=128).contains(&self.max_download_task) {
            return Err(invalid("Parallel downloads must be between 1 and 128"));
        }
        if !(1..=32).contains(&self.download_connections) {
            return Err(invalid("Connections per download must be between 1 and 32"));
        }
        if self.media_types.is_empty()
            || self.media_types.iter().any(|kind| {
                !matches!(
                    kind.as_str(),
                    "audio" | "photo" | "video" | "document" | "voice" | "video_note"
                )
            })
        {
            return Err(invalid("Select at least one supported media type"));
        }
        if self.file_path_prefix.iter().any(|part| {
            !matches!(
                part.as_str(),
                "chat_title" | "media_datetime" | "media_type"
            )
        }) {
            return Err(invalid(
                "Folder fields must be chat_title, media_datetime, or media_type",
            ));
        }
        if self
            .file_name_prefix
            .iter()
            .any(|part| !matches!(part.as_str(), "message_id" | "file_name" | "caption"))
        {
            return Err(invalid(
                "Filename fields must be message_id, file_name, or caption",
            ));
        }
        if self.file_name_prefix_split.contains(['/', '\\', '\0']) {
            return Err(invalid("Filename separator cannot contain path separators"));
        }
        if self.date_format.is_empty()
            || self.date_format.contains('\0')
            || chrono::format::StrftimeItems::new(&self.date_format)
                .any(|item| matches!(item, chrono::format::Item::Error))
        {
            return Err(invalid("Enter a valid date format, such as %Y_%m"));
        }
        for formats in [
            &self.file_formats.audio,
            &self.file_formats.video,
            &self.file_formats.document,
        ] {
            if formats.is_empty()
                || formats.iter().any(|ext| {
                    ext.is_empty() || !ext.bytes().all(|byte| byte.is_ascii_alphanumeric())
                })
            {
                return Err(invalid(
                    "Extensions must be letters or numbers without dots; use all for any extension",
                ));
            }
        }
        Ok(())
    }

    fn apply(self, cfg: &mut Config) {
        if let Some(chats) = self.chat {
            cfg.chat = chats.into_iter().map(|chat| chat.config).collect();
        }
        cfg.max_download_task = self.max_download_task;
        cfg.download_connections = self.download_connections;
        cfg.media_types = self.media_types;
        cfg.file_formats = self.file_formats;
        cfg.file_path_prefix = self.file_path_prefix;
        cfg.file_name_prefix = self.file_name_prefix;
        cfg.file_name_prefix_split = self.file_name_prefix_split;
        cfg.date_format = self.date_format;
    }
}

fn internal(error: anyhow::Error) -> Error {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

async fn snapshot(cfg: Config, state: &ApiState) -> Result<Settings, Error> {
    let cursors = crate::telegram::storage::cursors::load(&state.database)
        .await
        .map_err(internal)?;
    let mut settings = Settings::from(cfg);
    if let Some(chats) = &mut settings.chat {
        for chat in chats {
            chat.last_read_message_id = cursors.get(&chat.config.chat_id).copied();
        }
    }
    Ok(settings)
}

pub(super) async fn get(State(state): State<Arc<ApiState>>) -> Result<Json<Settings>, Error> {
    let _edit = state.config_update.lock().await;
    let cfg = FILE.load_optional().map_err(internal)?.unwrap_or_default();
    Ok(Json(snapshot(cfg, &state).await?))
}

pub(super) async fn put(
    State(state): State<Arc<ApiState>>,
    Json(mut settings): Json<Settings>,
) -> Result<Json<Settings>, Error> {
    settings.validate()?;
    let _edit = state.config_update.lock().await;
    let mut cfg = FILE.load_optional().map_err(internal)?.unwrap_or_default();
    let original = settings.original_chat.as_ref().map(|chats| {
        chats
            .iter()
            .map(|chat| chat.config.clone())
            .collect::<Vec<_>>()
    });
    if settings.chat.is_some() && original.as_ref() != Some(&cfg.chat) {
        return Err((StatusCode::CONFLICT, "Subscriptions changed since settings were loaded. Reload settings before editing chats.".into()));
    }
    let mut updates = Vec::new();
    for chat in settings.chat.iter().flatten() {
        let previous = settings
            .original_chat
            .iter()
            .flatten()
            .find(|old| old.config.chat_id == chat.config.chat_id)
            .and_then(|old| old.last_read_message_id);
        if let Some(cursor) = chat.last_read_message_id
            && Some(cursor) != previous
        {
            updates.push((chat.config.chat_id.clone(), cursor));
        }
    }
    settings.apply(&mut cfg);
    FILE.save(&cfg).map_err(internal)?;
    crate::telegram::storage::cursors::queue(&state.database, &updates)
        .await
        .map_err(internal)?;
    state.settings_changed.send_replace(());
    Ok(Json(snapshot(cfg, &state).await?))
}
