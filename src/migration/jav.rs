use super::{imported, mark_imported};
use crate::storage::{Connection, params};
use anyhow::Context;
use serde::Deserialize;
use std::{fs, path::Path};

pub(super) async fn import(tx: &Connection) -> anyhow::Result<()> {
    for path in [
        "jav-state.json",
        "config/jav-state.json",
        "config/state.json",
        "config/jav/state.json",
    ] {
        if Path::new(path).exists() && !imported(tx, path).await? {
            let text = fs::read_to_string(path).with_context(|| format!("cannot read {path}"))?;
            if !text.trim().is_empty() {
                let state: JavState = serde_json::from_str(&text)
                    .with_context(|| format!("invalid legacy state in {path}"))?;
                for record in state.records {
                    tx.execute("INSERT OR IGNORE INTO jav_records(id,url,title,rank,status,path,size,finished_at,error) VALUES (?,?,?,?,?,?,?,?,?)", params![record.id,record.url,record.title,record.rank.map(i64::try_from).transpose()?,record.status,record.path,i64::try_from(record.size)?,record.finished_at,record.error]).await?;
                }
                tx.execute(
                    "INSERT OR IGNORE INTO jav_scheduler(id,last_daily_run) VALUES (1,?)",
                    [state.last_daily_run],
                )
                .await?;
            }
            mark_imported(tx, path).await?;
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct JavState {
    #[serde(default)]
    records: Vec<JavRecord>,
    #[serde(default)]
    last_daily_run: Option<String>,
}
#[derive(Deserialize)]
struct JavRecord {
    id: String,
    url: String,
    title: String,
    status: String,
    finished_at: String,
    #[serde(default)]
    rank: Option<usize>,
    #[serde(default)]
    path: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    error: Option<String>,
}
