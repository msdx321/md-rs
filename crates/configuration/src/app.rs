//! Shared HTTP listener settings. Provider files contain only provider settings.
use serde::{Deserialize, Serialize};

pub const FILE: crate::ConfigFile<Config> =
    crate::ConfigFile::new("config/app.yaml").with_defaults();

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub host: String,
    pub history_retention_days: u32,
    pub telegram_download_path: std::path::PathBuf,
    pub jav_download_path: std::path::PathBuf,
    pub temp_path: std::path::PathBuf,
    pub port: u16,
    pub schedules: crate::schedule::Schedules,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            history_retention_days: 30,
            telegram_download_path: "downloads/telegram".into(),
            jav_download_path: "downloads/jav".into(),
            temp_path: "temp".into(),
            port: 8080,
            schedules: crate::schedule::Schedules::default(),
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
