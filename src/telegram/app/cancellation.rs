use std::{io::ErrorKind, path::Path};

use anyhow::Context;
use rustc_hash::FxHashMap;

use crate::telegram::storage::{ChatData, checkpoints};

/// Called only after all transfer writers have stopped. Only database-tracked
/// partial files are removed; completed media and history are untouched.
pub(super) async fn discard_pending(
    database: &crate::storage::Database,
    chats: &mut FxHashMap<String, ChatData>,
) -> anyhow::Result<()> {
    for path in checkpoints::paths(database).await? {
        anyhow::ensure!(
            path.extension().is_some_and(|ext| ext == "part"),
            "invalid partial download path: {}",
            path.display()
        );
        remove_if_present(&path).await?;
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(".progress");
        remove_if_present(Path::new(&sidecar)).await?;
        checkpoints::delete(&path, database).await?;
    }
    for chat in chats.values_mut() {
        chat.ids_to_retry.clear();
    }
    Ok(())
}

async fn remove_if_present(path: &Path) -> anyhow::Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("cannot delete {}", path.display())),
    }
}
