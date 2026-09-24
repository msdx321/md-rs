//! Shared HTTP listener settings. Provider files contain only provider settings.
use serde::{Deserialize, Serialize};

pub const FILE: crate::configuration::ConfigFile<Config> =
    crate::configuration::ConfigFile::new("config/app.yaml")
        .with_defaults()
        .with_groups(&[
            &["host", "port"],
            &[
                "jav_download_path",
                "p91_download_path",
                "telegram_download_path",
                "temp_path",
            ],
            &["history_retention_days"],
            &[
                "download_limit_mb_per_sec",
                "jav_download_limit_mb_per_sec",
                "p91_download_limit_mb_per_sec",
                "telegram_download_limit_mb_per_sec",
            ],
            &[
                "min_video_resolution",
                "telegram_min_video_resolution",
                "jav_min_video_resolution",
                "p91_min_video_resolution",
            ],
            &["schedules"],
        ]);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub host: String,
    pub download_limit_mb_per_sec: f64,
    pub telegram_download_limit_mb_per_sec: f64,
    pub jav_download_limit_mb_per_sec: f64,
    pub p91_download_limit_mb_per_sec: f64,

    /// Minimum shorter-side video dimension in pixels; 0 disables filtering.
    pub min_video_resolution: u32,
    /// None inherits the global minimum; Some(0) explicitly disables filtering.
    pub telegram_min_video_resolution: Option<u32>,
    pub jav_min_video_resolution: Option<u32>,
    pub p91_min_video_resolution: Option<u32>,
    pub history_retention_days: u32,
    pub telegram_download_path: std::path::PathBuf,
    pub jav_download_path: std::path::PathBuf,
    pub p91_download_path: std::path::PathBuf,
    pub temp_path: std::path::PathBuf,
    pub port: u16,
    pub schedules: crate::configuration::schedule::Schedules,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            history_retention_days: 30,
            min_video_resolution: 0,
            telegram_min_video_resolution: None,
            jav_min_video_resolution: None,
            p91_min_video_resolution: None,
            download_limit_mb_per_sec: 0.0,
            telegram_download_limit_mb_per_sec: 0.0,
            jav_download_limit_mb_per_sec: 0.0,
            p91_download_limit_mb_per_sec: 0.0,
            telegram_download_path: "downloads/telegram".into(),
            jav_download_path: "downloads/jav".into(),
            p91_download_path: "downloads/p91".into(),
            temp_path: "temp".into(),
            port: 8080,
            schedules: crate::configuration::schedule::Schedules::default(),
        }
    }
}

pub fn load_or_create() -> anyhow::Result<Config> {
    if let Some(config) = FILE.load_optional()? {
        return Ok(config);
    }
    // Preserve existing launch settings when creating the common config.
    // Once created, the YAML file is authoritative.
    let mut config = Config::default();
    if let Ok(host) = std::env::var("MEDIA_HOST") {
        config.host = host;
    }
    if let Ok(port) = std::env::var("MEDIA_PORT") {
        config.port = port.parse()?;
    }
    FILE.save(&config)?;
    Ok(config)
}

impl Config {
    pub fn minimum_video_resolution(
        &self,
        module: crate::runtime::download_limiter::DownloadModule,
    ) -> u32 {
        use crate::runtime::download_limiter::DownloadModule;
        match module {
            DownloadModule::Telegram => self.telegram_min_video_resolution,
            DownloadModule::Jav => self.jav_min_video_resolution,
            DownloadModule::P91 => self.p91_min_video_resolution,
        }
        .unwrap_or(self.min_video_resolution)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        for limit in [
            self.download_limit_mb_per_sec,
            self.telegram_download_limit_mb_per_sec,
            self.jav_download_limit_mb_per_sec,
            self.p91_download_limit_mb_per_sec,
        ] {
            anyhow::ensure!(
                limit == 0.0 || (0.000001..=u32::MAX as f64).contains(&limit),
                "Download limits must be 0 (unlimited) or between 0.000001 and 4294967295 MB/s"
            );
        }
        for minimum in [
            Some(self.min_video_resolution),
            self.telegram_min_video_resolution,
            self.jav_min_video_resolution,
            self.p91_min_video_resolution,
        ]
        .into_iter()
        .flatten()
        {
            anyhow::ensure!(
                minimum <= 16384,
                "Minimum video resolution must be between 0 and 16384 pixels"
            );
        }
        anyhow::ensure!(
            (1..=36500).contains(&self.history_retention_days),
            "History retention must be between 1 and 36500 days"
        );
        anyhow::ensure!(
            !self.host.trim().is_empty() && self.port != 0,
            "Host and port must be valid"
        );
        for path in [
            &self.telegram_download_path,
            &self.jav_download_path,
            &self.p91_download_path,
            &self.temp_path,
        ] {
            anyhow::ensure!(
                !path.as_os_str().is_empty(),
                "Directories must not be empty"
            );
        }
        self.schedules.telegram.validate()?;
        self.schedules.jav.validate()?;
        self.schedules.p91.validate()?;
        Ok(())
    }
}
