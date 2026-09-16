pub mod jav;
pub mod telegram;

use anyhow::Context;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

pub const DIRECTORY: &str = "config";
pub const TELEGRAM_FILE: &str = "config/telegram.yaml";
pub const JAV_FILE: &str = "config/jav.yaml";

/// A configuration file paired with its provider-owned data model.
pub struct ConfigFile<T> {
    path: &'static str,
    model: std::marker::PhantomData<fn() -> T>,
}

impl<T> ConfigFile<T> {
    pub const fn new(path: &'static str) -> Self {
        Self {
            path,
            model: std::marker::PhantomData,
        }
    }
}

impl<T: DeserializeOwned> ConfigFile<T> {
    pub fn load_optional(&self) -> anyhow::Result<Option<T>> {
        let text = match fs::read_to_string(self.path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).with_context(|| format!("cannot read {}", self.path)),
        };
        serde_yaml::from_str(&text)
            .with_context(|| format!("invalid YAML in {}", self.path))
            .map(Some)
    }
}

impl<T: Serialize> ConfigFile<T> {
    /// Save durably. Provider code serializes edits and publishes only after success.
    pub fn save(&self, config: &T) -> anyhow::Result<()> {
        save(self.path, config)
    }
}

/// Read an arbitrary YAML path, including legacy migration candidates.
pub fn load<T: DeserializeOwned>(path: impl AsRef<Path>) -> anyhow::Result<T> {
    let path = path.as_ref();
    let text =
        fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    serde_yaml::from_str(&text).with_context(|| format!("invalid YAML in {}", path.display()))
}

/// Callers serialize writes to the same configuration.
pub fn save<T: Serialize>(path: impl AsRef<Path>, config: &T) -> anyhow::Result<()> {
    write_atomic(path.as_ref(), serde_yaml::to_string(config)?.as_bytes())
}

/// Replace a configuration file only after its new contents have reached disk.
/// Callers serialize writes to each destination.
fn write_atomic(path: &Path, contents: &[u8]) -> anyhow::Result<()> {
    let mut temp = path.as_os_str().to_owned();
    temp.push(".tmp");
    let temp = PathBuf::from(temp);
    let result = (|| -> std::io::Result<()> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        if let Ok(metadata) = fs::metadata(path) {
            file.set_permissions(metadata.permissions())?;
        }
        file.write_all(contents)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        #[cfg(unix)]
        fs::File::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?
        .sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.with_context(|| format!("cannot save {}", path.display()))
}
