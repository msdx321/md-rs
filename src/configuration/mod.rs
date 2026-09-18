pub mod app;
pub mod jav;
pub mod p91;
pub mod schedule;
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
pub const P91_FILE: &str = "config/p91.yaml";

/// A configuration file paired with its provider-owned data model.
pub struct ConfigFile<T> {
    keep_defaults: bool,
    groups: &'static [&'static [&'static str]],
    path: &'static str,
    model: std::marker::PhantomData<fn() -> T>,
}

impl<T> ConfigFile<T> {
    pub const fn new(path: &'static str) -> Self {
        Self {
            path,
            keep_defaults: false,
            groups: &[],
            model: std::marker::PhantomData,
        }
    }

    /// Keep shared settings explicit, including migration precedence markers.
    pub const fn with_defaults(mut self) -> Self {
        self.keep_defaults = true;
        self
    }

    /// Root fields grouped in display order; keys within each group are sorted.
    pub const fn with_groups(mut self, groups: &'static [&'static [&'static str]]) -> Self {
        self.groups = groups;
        self
    }
}

// Normalizing a read writes the file, so it must serialize with saves.
static CONFIG_IO: std::sync::Mutex<()> = std::sync::Mutex::new(());

impl<T: DeserializeOwned + Serialize + Default> ConfigFile<T> {
    pub fn load_optional(&self) -> anyhow::Result<Option<T>> {
        let _io = CONFIG_IO.lock().unwrap_or_else(|error| error.into_inner());
        let text = match fs::read_to_string(self.path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).with_context(|| format!("cannot read {}", self.path)),
        };
        let config = serde_yaml::from_str(&text)
            .with_context(|| format!("invalid YAML in {}", self.path))?;
        let normalized = self.render(&config)?;
        if normalized != text {
            write_atomic(Path::new(self.path), normalized.as_bytes())?;
        }
        Ok(Some(config))
    }
}

impl<T: Serialize + Default> ConfigFile<T> {
    /// Save normalized YAML durably, omitting defaults unless explicitly retained.
    /// Provider code serializes edits and publishes only after success.
    pub fn save(&self, config: &T) -> anyhow::Result<()> {
        let _io = CONFIG_IO.lock().unwrap_or_else(|error| error.into_inner());
        write_atomic(Path::new(self.path), self.render(config)?.as_bytes())
    }

    fn render(&self, config: &T) -> anyhow::Result<String> {
        let mut value = serde_yaml::to_value(config)?;
        let defaults = serde_yaml::to_value(T::default())?;
        if !self.keep_defaults
            && let (Some(fields), Some(defaults)) = (value.as_mapping_mut(), defaults.as_mapping())
        {
            fields.retain(|key, value| defaults.get(key) != Some(value));
        }
        sorted_yaml(value, self.groups)
    }
}

/// Read raw legacy data without normalization, preserving fields for migration.
pub fn load<T: DeserializeOwned>(path: impl AsRef<Path>) -> anyhow::Result<T> {
    let path = path.as_ref();
    let text =
        fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    serde_yaml::from_str(&text).with_context(|| format!("invalid YAML in {}", path.display()))
}

/// Sort mappings recursively; sequence order is meaningful and stays unchanged.
fn sorted_yaml(mut value: serde_yaml::Value, groups: &[&[&str]]) -> anyhow::Result<String> {
    fn sort(value: &mut serde_yaml::Value) {
        match value {
            serde_yaml::Value::Mapping(mapping) => {
                let mut entries: Vec<_> = std::mem::take(mapping).into_iter().collect();
                entries.sort_by(|(left, _), (right, _)| left.as_str().cmp(&right.as_str()));
                for (key, mut value) in entries {
                    sort(&mut value);
                    mapping.insert(key, value);
                }
            }
            serde_yaml::Value::Sequence(items) => items.iter_mut().for_each(sort),
            serde_yaml::Value::Tagged(tagged) => sort(&mut tagged.value),
            _ => {}
        }
    }
    sort(&mut value);
    if !groups.is_empty()
        && let serde_yaml::Value::Mapping(fields) = &value
        && !fields.is_empty()
    {
        let mut sections = vec![serde_yaml::Mapping::new(); groups.len() + 1];
        // Fields are already alphabetical. Unknown future fields go last.
        for (key, value) in fields {
            let group = groups
                .iter()
                .position(|group| group.contains(&key.as_str().unwrap_or_default()))
                .unwrap_or(groups.len());
            sections[group].insert(key.clone(), value.clone());
        }
        let sections = sections
            .iter()
            .filter(|section| !section.is_empty())
            .map(serde_yaml::to_string)
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(sections.join("\n"));
    }
    Ok(serde_yaml::to_string(&value)?)
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
