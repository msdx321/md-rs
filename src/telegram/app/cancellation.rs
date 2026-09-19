use std::{io::ErrorKind, path::Path};

use anyhow::Context;
use rustc_hash::FxHashMap;

use crate::telegram::storage::{ChatData, checkpoints};

/// Called only after all transfer writers have stopped. Only database-tracked
/// partial files are removed; completed media and history are untouched.
pub(super) async fn discard_pending(
    database: &crate::storage::Database,
    chats: &mut FxHashMap<String, ChatData>,
    roots: &[&Path],
) -> anyhow::Result<usize> {
    let mut retained = 0;
    for path in checkpoints::paths(database).await? {
        let sidecar = crate::telegram::downloader::paths::appended(&path, ".progress");
        let safe = path.extension().is_some_and(|ext| ext == "part")
            && roots.iter().any(|root| {
                crate::telegram::downloader::paths::ensure_contained(root, &path).is_ok()
                    && crate::telegram::downloader::paths::ensure_contained(root, &sidecar).is_ok()
            });
        if !safe {
            log::warn!(
                "retaining unsafe legacy partial/checkpoint: {}",
                path.display()
            );
            retained += 1;
            continue;
        }
        remove_if_present(&path).await?;
        remove_if_present(Path::new(&sidecar)).await?;
        checkpoints::delete(&path, database).await?;
    }
    for chat in chats.values_mut() {
        chat.ids_to_retry.clear();
    }
    Ok(retained)
}

async fn remove_if_present(path: &Path) -> anyhow::Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("cannot delete {}", path.display())),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[tokio::test]
    async fn cancellation_removes_safe_partials_but_retains_unsafe_legacy_paths_and_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("temp");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let sentinel = outside.join("sentinel.part");
        std::fs::write(&sentinel, b"untouched").unwrap();
        let safe = root.join("safe.part");
        std::fs::write(&safe, b"safe").unwrap();
        std::fs::write(root.join("safe.part.progress"), b"4").unwrap();
        symlink(&outside, root.join("link")).unwrap();
        symlink(&sentinel, root.join("symlink.part")).unwrap();
        let sidecar = root.join("sidecar.part");
        std::fs::write(&sidecar, b"keep too").unwrap();
        symlink(&sentinel, root.join("sidecar.part.progress")).unwrap();
        let unsafe_paths = [
            sentinel.clone(),
            root.join("../outside/sentinel.part"),
            root.join("link/sentinel.part"),
            root.join("symlink.part"),
            sidecar.clone(),
            root.join("C:\\outside.part"),
        ];
        let db = crate::storage::Database::open(":memory:").await.unwrap();
        for path in unsafe_paths.iter().chain([&safe]) {
            checkpoints::write_progress(path, 4, &db).await.unwrap();
        }
        assert_eq!(
            discard_pending(&db, &mut FxHashMap::default(), &[&root])
                .await
                .unwrap(),
            unsafe_paths.len()
        );
        assert!(!safe.exists());
        assert!(!root.join("safe.part.progress").exists());
        assert_eq!(std::fs::read(&sentinel).unwrap(), b"untouched");
        assert_eq!(std::fs::read(&sidecar).unwrap(), b"keep too");
        assert!(
            std::fs::symlink_metadata(root.join("symlink.part"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        let remaining = checkpoints::paths(&db).await.unwrap();
        assert_eq!(remaining.len(), unsafe_paths.len());
        for path in unsafe_paths {
            assert!(remaining.contains(&std::path::absolute(path).unwrap()));
        }
    }
}
