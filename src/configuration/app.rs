//! Shared HTTP listener settings. Provider files contain only provider settings.
use serde::{Deserialize, Serialize};

pub const FILE: crate::configuration::ConfigFile<Config> =
    crate::configuration::ConfigFile::new("config/app.yaml")
        .with_defaults()
        .with_groups(&[
            &["host", "port"],
            &["jav_download_path", "telegram_download_path", "temp_path"],
            &["history_retention_days"],
            &[
                "download_limit_mb_per_sec",
                "telegram_download_limit_mb_per_sec",
                "jav_download_limit_mb_per_sec",
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

    pub history_retention_days: u32,
    pub telegram_download_path: std::path::PathBuf,
    pub jav_download_path: std::path::PathBuf,
    pub temp_path: std::path::PathBuf,
    pub port: u16,
    pub schedules: crate::configuration::schedule::Schedules,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            history_retention_days: 30,
            download_limit_mb_per_sec: 0.0,
            telegram_download_limit_mb_per_sec: 0.0,
            jav_download_limit_mb_per_sec: 0.0,
            telegram_download_path: "downloads/telegram".into(),
            jav_download_path: "downloads/jav".into(),
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
    pub fn validate(&self) -> anyhow::Result<()> {
        for limit in [
            self.download_limit_mb_per_sec,
            self.telegram_download_limit_mb_per_sec,
            self.jav_download_limit_mb_per_sec,
        ] {
            anyhow::ensure!(
                limit == 0.0 || (0.000001..=u32::MAX as f64).contains(&limit),
                "Download limits must be 0 (unlimited) or between 0.000001 and 4294967295 MB/s"
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
            &self.temp_path,
        ] {
            anyhow::ensure!(
                !path.as_os_str().is_empty(),
                "Directories must not be empty"
            );
        }
        self.schedules.telegram.validate()?;
        self.schedules.jav.validate()?;
        Ok(())
    }
}
