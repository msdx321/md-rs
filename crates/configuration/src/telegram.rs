use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// User-editable configuration loaded from `config/telegram.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub api_id: i32,
    #[serde(default)]
    pub api_hash: String,
    #[serde(default)]
    pub chat: Vec<ChatConfig>,
    #[serde(default = "default_media_types")]
    pub media_types: Vec<String>,
    #[serde(default)]
    pub file_formats: FileFormats,
    #[serde(skip)]
    pub save_path: PathBuf,
    #[serde(skip)]
    pub temp_path: PathBuf,
    #[serde(default = "default_file_path_prefix")]
    pub file_path_prefix: Vec<String>,
    #[serde(default = "default_file_name_prefix")]
    pub file_name_prefix: Vec<String>,
    #[serde(default = "default_file_name_prefix_split")]
    pub file_name_prefix_split: String,
    #[serde(default = "default_max_download_task")]
    pub max_download_task: usize,
    #[serde(default = "default_download_connections")]
    pub download_connections: usize,
    #[serde(default = "default_date_format")]
    pub date_format: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_id: 0,
            api_hash: String::new(),
            chat: Vec::new(),
            media_types: default_media_types(),
            file_formats: FileFormats::default(),
            save_path: default_save_path(),
            temp_path: "temp/telegram".into(),
            file_path_prefix: default_file_path_prefix(),
            file_name_prefix: default_file_name_prefix(),
            file_name_prefix_split: default_file_name_prefix_split(),
            max_download_task: default_max_download_task(),
            download_connections: default_download_connections(),
            date_format: default_date_format(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatConfig {
    pub chat_id: String,
    #[serde(default)]
    pub download_filter: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileFormats {
    #[serde(default = "default_all")]
    pub audio: Vec<String>,
    #[serde(default = "default_all")]
    pub document: Vec<String>,
    #[serde(default = "default_all")]
    pub video: Vec<String>,
}

impl Default for FileFormats {
    fn default() -> Self {
        Self {
            audio: default_all(),
            document: default_all(),
            video: default_all(),
        }
    }
}

fn default_media_types() -> Vec<String> {
    vec![
        "audio".into(),
        "photo".into(),
        "video".into(),
        "document".into(),
        "voice".into(),
        "video_note".into(),
    ]
}

fn default_save_path() -> PathBuf {
    PathBuf::from("downloads/telegram")
}

fn default_file_path_prefix() -> Vec<String> {
    vec!["chat_title".into(), "media_datetime".into()]
}

fn default_file_name_prefix() -> Vec<String> {
    vec!["message_id".into(), "file_name".into()]
}

fn default_file_name_prefix_split() -> String {
    " - ".into()
}

fn default_date_format() -> String {
    "%Y_%m".into()
}

fn default_all() -> Vec<String> {
    vec!["all".into()]
}

fn default_max_download_task() -> usize {
    5
}

fn default_download_connections() -> usize {
    4
}

pub const FILE: crate::ConfigFile<Config> = crate::ConfigFile::new(crate::TELEGRAM_FILE);
