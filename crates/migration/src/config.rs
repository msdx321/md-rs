use anyhow::{Context, bail};
use media_config::load as read_yaml;
use serde_yaml::Value;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

pub(super) fn discover_config(engine: &str, destination: &str) -> anyhow::Result<Option<PathBuf>> {
    if Path::new(destination).exists() {
        return Ok(None);
    }
    let root = PathBuf::from(format!("{engine}.yaml"));
    if root.exists() {
        let _: Value = read_yaml(&root)?;
        return Ok(Some(root));
    }
    let mut candidates = Vec::new();
    let nested = PathBuf::from(format!("config/{engine}/config.yaml"));
    if nested.exists() {
        candidates.push(nested);
    }
    let flat = Path::new("config/config.yaml");
    if flat.exists() {
        let value: Value = read_yaml(flat)?;
        let telegram = ["api_id", "api_hash", "chat"]
            .iter()
            .any(|key| value.get(*key).is_some());
        let jav = [
            "site_base",
            "popular_path",
            "links",
            "cookie",
            "daily_enabled",
            "daily_time",
            "top_n",
            "browser_enabled",
            "resolution",
            "segment_concurrency",
        ]
        .iter()
        .any(|key| value.get(*key).is_some());
        anyhow::ensure!(
            telegram != jav,
            "cannot identify config/config.yaml; use config/telegram/config.yaml or config/jav/config.yaml"
        );
        if (engine == "telegram") == telegram {
            candidates.push(flat.to_path_buf());
        }
    }
    if candidates.len() > 1 {
        bail!("multiple legacy {engine} configs found; keep one or provide {destination}");
    }
    if let Some(path) = candidates.first() {
        let _: Value = read_yaml(path)?;
    }
    Ok(candidates.pop())
}

pub(super) fn install_config(source: &Path, destination: &Path) -> anyhow::Result<()> {
    if destination.exists() {
        return Ok(());
    }
    let bytes = fs::read(source)?;
    let temporary = destination.with_extension("yaml.migration.tmp");
    let result = (|| -> anyhow::Result<()> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        // Atomic no-clobber installation, even if a config appeared while importing.
        match fs::hard_link(&temporary, destination) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    })();
    let _ = fs::remove_file(temporary);
    result.with_context(|| {
        format!(
            "cannot migrate {} to {}",
            source.display(),
            destination.display()
        )
    })
}
