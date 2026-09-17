use std::{io, path::Path};

/// Run only after transfer writers have stopped. Keep the configured root,
/// partial files needed for resume, and symlinks untouched.
pub(super) async fn clean_empty_dirs(root: &Path) {
    if let Err(error) = remove_empty_dirs(root).await {
        log::warn!(
            "cannot clean empty temporary directories in {}: {error}",
            root.display()
        );
    }
}

async fn remove_empty_dirs(root: &Path) -> io::Result<()> {
    let mut entries = match tokio::fs::read_dir(root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let path = entry.path();
        Box::pin(remove_empty_dirs(&path)).await?;
        match tokio::fs::remove_dir(&path).await {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
                ) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
