//! Durable upload checkpoints and unfinished downloads for followed profiles.

use std::collections::HashSet;
use std::sync::atomic::Ordering;

use serde::{Deserialize, Serialize};

use crate::ph::app::AppCtx;
use crate::ph::config::Config;
use crate::ph::source::{
    http::Fetcher,
    scraper::{self, VideoCard},
};

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Subscription {
    checkpoint: Vec<String>,
    pub pending: Vec<VideoCard>,
}

impl Subscription {
    pub async fn load(ctx: &AppCtx, source: &str) -> anyhow::Result<Self> {
        ctx.subscription(source)
            .await?
            .map(|json| serde_json::from_str(&json))
            .transpose()
            .map(|state| state.unwrap_or_default())
            .map_err(Into::into)
    }

    pub async fn save(&self, ctx: &AppCtx, source: &str) -> anyhow::Result<()> {
        ctx.save_subscription(source, &serde_json::to_string(self)?)
            .await
    }

    pub async fn refresh(
        &mut self,
        ctx: &AppCtx,
        source: &str,
        cfg: &Config,
        fetch: &Fetcher,
    ) -> anyhow::Result<()> {
        let initial = self.checkpoint.is_empty();
        let known: HashSet<_> = self.checkpoint.iter().collect();
        let mut pending_ids: HashSet<_> = self.pending.iter().map(|card| card.id.clone()).collect();
        let mut updated = self.clone();
        let mut added = 0;
        let mut reached_checkpoint = false;
        let mut page = Some(1);
        while let Some(current) = page.filter(|page| *page <= cfg.max_pages) {
            anyhow::ensure!(
                !ctx.jobs.is_closed() && !ctx.daily_cancel.load(Ordering::Acquire),
                "upload scan stopped"
            );
            let listing = scraper::fetch_popular(ctx.session(), fetch, cfg, current).await?;
            if current == 1 {
                updated.checkpoint = listing.ids.clone();
            }
            let new_ids: HashSet<_> = listing
                .ids
                .iter()
                .take_while(|id| !known.contains(id))
                .collect();
            reached_checkpoint = new_ids.len() < listing.ids.len();
            for card in listing.cards {
                if new_ids.contains(&card.id) && pending_ids.insert(card.id.clone()) {
                    updated.pending.push(card);
                    added += 1;
                    if initial && added >= cfg.top_n {
                        break;
                    }
                }
            }
            if reached_checkpoint || (initial && added >= cfg.top_n) {
                break;
            }
            page = listing.next_page;
        }
        // Never move the checkpoint past an unscanned gap. Keep the previous
        // pending queue intact when the page cap or removed uploads hide it.
        anyhow::ensure!(
            initial || reached_checkpoint,
            "previous uploads were not found within the page limit; increase the scan limit before syncing again"
        );
        anyhow::ensure!(
            !ctx.jobs.is_closed() && !ctx.daily_cancel.load(Ordering::Acquire),
            "upload scan stopped"
        );
        fetch.check_current()?;
        updated.save(ctx, source).await?;
        *self = updated;
        Ok(())
    }

    pub async fn finish(&mut self, ctx: &AppCtx, source: &str, id: &str) -> anyhow::Result<()> {
        let mut updated = self.clone();
        updated.pending.retain(|card| card.id != id);
        updated.save(ctx, source).await?;
        *self = updated;
        Ok(())
    }
}
