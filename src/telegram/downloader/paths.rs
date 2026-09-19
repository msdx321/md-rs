use std::path::{Component, Path, PathBuf};

use grammers_client::media::Media;

use crate::telegram::config::Config;
use crate::telegram::format::{truncate_filename, validate_title};

use super::metadata::media_kind_and_ext;

/// Remote strings are single components, never paths (including on Windows).
pub(super) fn safe_component(value: &str) -> String {
    let value: String = validate_title(value)
        .chars()
        .map(|c| if c.is_control() { '_' } else { c })
        .collect();
    if value.is_empty() || value.chars().all(|c| c == '.' || c == ' ') {
        "_".into()
    } else {
        value
    }
}

fn bounded_component(value: &str, limit: usize) -> String {
    safe_component(value)
        .chars()
        .scan(0, |bytes, c| {
            *bytes += c.len_utf8();
            (*bytes <= limit).then_some(c)
        })
        .collect()
}

pub(crate) fn appended(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    value.into()
}

/// Configured roots are trusted; descendants must be lexical children and must
/// not traverse existing symlinks, including a symlink at the file itself.
/// This is not a sandbox against concurrent hostile local filesystem mutation.
pub(crate) fn ensure_contained(root: &Path, path: &Path) -> anyhow::Result<()> {
    let root = std::path::absolute(root)?;
    let path = std::path::absolute(path)?;
    let relative = path.strip_prefix(&root)?;
    anyhow::ensure!(
        !relative.as_os_str().is_empty(),
        "path is the configured root"
    );
    let mut current = root;
    for part in relative.components() {
        let Component::Normal(name) = part else {
            anyhow::bail!("unsafe download path: {}", path.display());
        };
        // Reject foreign-platform prefixes/separators in persisted legacy names too.
        anyhow::ensure!(
            !name.to_string_lossy().contains(['\\', ':']),
            "unsafe legacy path: {}",
            path.display()
        );
        current.push(name);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) => anyhow::ensure!(
                !metadata.file_type().is_symlink(),
                "symlink in download path: {}",
                current.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub(super) struct MediaPaths {
    pub(super) temp: PathBuf,
    pub(super) final_path: PathBuf,
    temp_root: PathBuf,
    final_root: PathBuf,
}

impl MediaPaths {
    pub(super) fn validate_temp(&self) -> anyhow::Result<()> {
        ensure_contained(&self.temp_root, &self.temp)?;
        ensure_contained(&self.temp_root, &appended(&self.temp, ".progress"))
    }

    pub(super) fn validate_final(&self) -> anyhow::Result<()> {
        ensure_contained(&self.final_root, &self.final_path)?;
        ensure_contained(&self.final_root, &appended(&self.final_path, ".part"))?;
        ensure_contained(
            &self.final_root,
            &appended(&self.final_path, ".part.progress"),
        )
    }
}

/// Build safe paths while retaining the exact mapping for ordinary filenames.
pub(super) fn build_media_paths(
    msg: &grammers_client::message::Message,
    media: &Media,
    cfg: &Config,
) -> anyhow::Result<MediaPaths> {
    let chat_title = msg
        .peer()
        .and_then(|p| p.name())
        .map(safe_component)
        .unwrap_or_else(|| format!("{:?}", msg.peer_id()));

    media_paths(
        msg.id(),
        &chat_title,
        msg.date().naive_utc(),
        msg.text(),
        media,
        cfg,
    )
}

fn media_paths(
    msg_id: i32,
    chat_title: &str,
    date: chrono::NaiveDateTime,
    caption: &str,
    media: &Media,
    cfg: &Config,
) -> anyhow::Result<MediaPaths> {
    use std::fmt::Write;
    let mut datetime_str = String::new();
    write!(&mut datetime_str, "{}", date.format(&cfg.date_format))
        .map_err(|_| anyhow::anyhow!("invalid media date format"))?;
    let Some((media_type_str, ext)) = media_kind_and_ext(media) else {
        return Err(anyhow::anyhow!("unsupported media"));
    };

    let mut dir: PathBuf = cfg.save_path.clone();
    for seg in &cfg.file_path_prefix {
        match seg.as_str() {
            "chat_title" => dir.push(bounded_component(chat_title, 255)),
            "media_datetime" => {
                // Date formats are trusted configuration, not remote filenames.
                // Preserve safe nested mappings (e.g. %Y/%m) for existing resumes.
                let prefix = Path::new(&datetime_str);
                anyhow::ensure!(
                    !prefix.is_absolute()
                        && !datetime_str.contains(['\\', ':'])
                        && !datetime_str.chars().any(char::is_control)
                        && prefix
                            .components()
                            .all(|part| matches!(part, Component::Normal(_) | Component::CurDir)),
                    "unsafe media date prefix: {datetime_str}"
                );
                dir.push(prefix);
            }
            "media_type" => dir.push(media_type_str),
            _ => {}
        }
    }

    let stem = match media {
        Media::Photo(_) => msg_id.to_string(),
        Media::Document(doc) => {
            let mut stem = doc
                .name()
                .map(|name| name.rsplit_once('.').map_or(name, |(stem, _)| stem))
                .unwrap_or_default()
                .to_string();
            if stem.is_empty() {
                stem = format!("file_{}", doc.id());
            }
            let mut parts: Vec<String> = Vec::new();
            for seg in &cfg.file_name_prefix {
                match seg.as_str() {
                    "message_id" => parts.push(msg_id.to_string()),
                    "file_name" => parts.push(safe_component(&stem)),
                    "caption" => {
                        let txt = caption;
                        if !txt.is_empty() {
                            parts.push(safe_component(txt));
                        }
                    }
                    _ => {}
                }
            }
            let sep = &cfg.file_name_prefix_split;
            if parts.is_empty() {
                msg_id.to_string()
            } else {
                parts.join(sep)
            }
        }
        _ => return Err(anyhow::anyhow!("unsupported media")),
    };

    let stem = safe_component(&stem);
    // Bound the extension too; truncate_filename historically only bounds the stem.
    let ext = bounded_component(&ext, 64);
    let fname = format!("{stem}.{ext}");
    let final_path = truncate_filename(&dir.join(&fname), 230);

    let temp_path = cfg
        .temp_path
        .join(final_path.strip_prefix(&cfg.save_path)?)
        .with_extension(format!("{ext}.part"));

    let paths = MediaPaths {
        temp: temp_path,
        final_path,
        temp_root: cfg.temp_path.clone(),
        final_root: cfg.save_path.clone(),
    };
    paths.validate_temp()?;
    paths.validate_final()?;
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use grammers_client::{media::Document, tl};

    fn document(name: &str) -> Media {
        Media::Document(Document::from_raw_media(tl::types::MessageMediaDocument {
            nopremium: false,
            spoiler: false,
            video: false,
            round: false,
            voice: false,
            document: Some(
                tl::types::Document {
                    id: 42,
                    access_hash: 0,
                    file_reference: vec![],
                    date: 0,
                    mime_type: "application/pdf".into(),
                    size: 9,
                    thumbs: None,
                    video_thumbs: None,
                    dc_id: 0,
                    attributes: vec![
                        tl::types::DocumentAttributeFilename {
                            file_name: name.into(),
                        }
                        .into(),
                    ],
                }
                .into(),
            ),
            alt_documents: None,
            video_cover: None,
            video_timestamp: None,
            ttl_seconds: None,
        }))
    }

    fn config(root: &Path) -> Config {
        Config {
            save_path: root.join("save"),
            temp_path: root.join("temp"),
            ..Config::default()
        }
    }

    fn paths(cfg: &Config, chat: &str, caption: &str, name: &str) -> anyhow::Result<MediaPaths> {
        media_paths(
            7,
            chat,
            chrono::NaiveDate::from_ymd_opt(2026, 9, 5)
                .unwrap()
                .and_hms_opt(12, 30, 0)
                .unwrap(),
            caption,
            &document(name),
            cfg,
        )
    }

    #[tokio::test]
    async fn remote_components_are_confined_and_safe_legacy_mapping_resumes() {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(root.path());
        let safe = paths(&cfg, "旅行", "", "报告.pdf").unwrap();
        assert_eq!(
            safe.final_path,
            cfg.save_path.join("旅行/2026_09/7 - 报告.pdf")
        );
        assert_eq!(
            safe.temp,
            cfg.temp_path.join("旅行/2026_09/7 - 报告.pdf.part")
        );
        std::fs::create_dir_all(safe.final_path.parent().unwrap()).unwrap();
        std::fs::create_dir_all(safe.temp.parent().unwrap()).unwrap();
        let legacy = appended(&safe.final_path, ".part");
        std::fs::write(&legacy, b"safe-data").unwrap();
        std::fs::write(appended(&legacy, ".progress"), b"9").unwrap();
        let db = crate::storage::Database::open(":memory:").await.unwrap();
        safe.validate_temp().unwrap();
        safe.validate_final().unwrap();
        crate::migration::relocate_partial(&safe.final_path, &safe.temp, &db)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&safe.temp).unwrap(), b"safe-data");
        assert_eq!(
            super::super::progress::resume_offset(&safe.temp, 9, &db)
                .await
                .unwrap(),
            9
        );
        assert!(!legacy.exists());
        let sentinel = root.path().join("sentinel");
        std::fs::write(&sentinel, b"untouched").unwrap();
        for name in [
            "../../sentinel",
            "/absolute.pdf",
            "C:\\outside\\name.pdf",
            "stem.ext/../../sentinel",
            "..",
            "",
            "x.\\evil",
            "a.\0pdf",
        ] {
            let mut cfg = cfg.clone();
            cfg.file_name_prefix = vec!["file_name".into(), "caption".into()];
            cfg.file_name_prefix_split = "/../\\".into();
            let item = paths(&cfg, "..", "../caption", name).unwrap();
            item.validate_temp().unwrap();
            item.validate_final().unwrap();
            std::fs::create_dir_all(item.temp.parent().unwrap()).unwrap();
            std::fs::write(&item.temp, b"fixture").unwrap();
            assert!(
                !item
                    .final_path
                    .strip_prefix(&cfg.save_path)
                    .unwrap()
                    .components()
                    .any(|c| !matches!(c, Component::Normal(_)))
            );
        }
        assert_eq!(std::fs::read(sentinel).unwrap(), b"untouched");
        let huge = format!("{}.{}", "界".repeat(400), "語".repeat(400));
        let item = paths(&cfg, &"界".repeat(400), "", &huge).unwrap();
        assert!(item.final_path.file_name().unwrap().len() <= 230);
        assert!(item.temp.file_name().unwrap().len() <= 235);
        for format in [
            "%Q",
            "../%Y",
            "/%Y/%m",
            "%Y/../outside",
            "C:/%Y",
            "%Y\\%m",
            "\\\\host\\share",
        ] {
            let mut invalid = cfg.clone();
            invalid.date_format = format.into();
            assert!(paths(&invalid, "safe", "", "safe.pdf").is_err(), "{format}");
        }
        let mut nested = cfg.clone();
        nested.date_format = "%Y/%m".into();
        let item = paths(&nested, "旅行", "", "报告.pdf").unwrap();
        assert_eq!(
            item.final_path,
            cfg.save_path.join("旅行/2026/09/7 - 报告.pdf")
        );
        assert_eq!(
            item.temp,
            cfg.temp_path.join("旅行/2026/09/7 - 报告.pdf.part")
        );
        std::fs::create_dir_all(item.final_path.parent().unwrap()).unwrap();
        std::fs::create_dir_all(item.temp.parent().unwrap()).unwrap();
        let legacy = appended(&item.final_path, ".part");
        std::fs::write(&legacy, b"hierarchy").unwrap();
        std::fs::write(appended(&legacy, ".progress"), b"9").unwrap();
        crate::migration::relocate_partial(&item.final_path, &item.temp, &db)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&item.temp).unwrap(), b"hierarchy");
        assert_eq!(
            super::super::progress::resume_offset(&item.temp, 9, &db)
                .await
                .unwrap(),
            9
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn existing_symlinks_block_writes_migration_and_finalization_without_touching_sentinels()
    {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let cfg = config(root.path());
        std::fs::create_dir_all(&cfg.temp_path).unwrap();
        std::fs::create_dir_all(&cfg.save_path).unwrap();
        let outside = root.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        let sentinel = outside.join("sentinel.part");
        std::fs::write(&sentinel, b"sentinel").unwrap();
        symlink(&outside, cfg.temp_path.join("evil")).unwrap();
        assert!(paths(&cfg, "evil", "", "sentinel.pdf").is_err());
        symlink(&outside, cfg.save_path.join("evil")).unwrap();
        assert!(paths(&cfg, "evil", "", "sentinel.pdf").is_err());
        let item = paths(&cfg, "safe", "", "safe.pdf").unwrap();
        std::fs::create_dir_all(item.temp.parent().unwrap()).unwrap();
        std::fs::create_dir_all(item.final_path.parent().unwrap()).unwrap();
        let db = crate::storage::Database::open(":memory:").await.unwrap();
        let common = tokio::sync::watch::channel(crate::configuration::app::Config::default()).1;
        let state = std::sync::Arc::new(
            crate::telegram::api::ApiState::new(
                tokio::sync::mpsc::channel(1).0,
                db,
                common.clone(),
                std::sync::Arc::new(crate::runtime::download_limiter::DownloadLimiter::new(
                    common,
                )),
            )
            .await
            .unwrap(),
        );
        for target in [
            &item.temp,
            &item.final_path,
            &appended(&item.final_path, ".part"),
            &appended(&item.final_path, ".part.progress"),
            &appended(&item.temp, ".progress"),
        ] {
            symlink(&sentinel, target).unwrap();
            assert!(paths(&cfg, "safe", "", "safe.pdf").is_err());
            let ids =
                std::sync::Arc::new(tokio::sync::Mutex::new(rustc_hash::FxHashMap::default()));
            assert!(
                super::super::finalize::finalize_download(7, "42", &ids, &item, 8, &state)
                    .await
                    .is_err()
            );
            if target == &item.temp {
                super::super::finalize::discard_partial(&item, &state.database).await;
                assert!(
                    std::fs::symlink_metadata(target)
                        .unwrap()
                        .file_type()
                        .is_symlink()
                );
            }
            std::fs::remove_file(target).unwrap();
            assert_eq!(std::fs::read(&sentinel).unwrap(), b"sentinel");
        }
        assert!(
            ensure_contained(
                &cfg.temp_path,
                &cfg.temp_path.join("../outside/sentinel.part")
            )
            .is_err()
        );
        assert!(ensure_contained(&cfg.temp_path, &sentinel).is_err());
    }
}
