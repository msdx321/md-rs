//! The daily job: rank the popular listing, skip what is already on disk and
//! download the remainder, then record the run.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use chrono::Local;
#[cfg(test)]
use chrono::{Duration as ChronoDuration, TimeZone};
#[cfg(test)]
use media_runtime::schedule::{next_occurrence, parse_daily_time};
use serde::Serialize;
use tokio::task::JoinSet;

use crate::app::{AppCtx, TaskInfo, TaskState, now_rfc3339};
use crate::config::Config;
use crate::downloader::download_video;
use crate::source::scraper;

#[derive(Debug, Default, Clone, Serialize)]
pub struct DailyReport {
    pub candidates: usize,
    pub skipped: usize,
    pub attempted: usize,
    pub completed: usize,
    pub failed: usize,
}

struct LinkQueue {
    cfg: Config,
    report: DailyReport,
    candidates: VecDeque<scraper::VideoCard>,
    queued: VecDeque<scraper::VideoCard>,
    seen: HashSet<String>,
    next_page: usize,
    in_flight: usize,
    error: Option<String>,
}

impl LinkQueue {
    fn new(cfg: Config) -> Self {
        Self {
            cfg,
            report: DailyReport::default(),
            candidates: VecDeque::new(),
            queued: VecDeque::new(),
            seen: HashSet::new(),
            next_page: 1,
            in_flight: 0,
            error: None,
        }
    }

    /// Reserve only enough candidates to fill this link's remaining quota.
    /// Registration makes waiting videos visible before workers start them.
    async fn fill(&mut self, ctx: &AppCtx, stopped: &mut bool) {
        let fetch = ctx.fetcher();
        while !*stopped
            && self.report.completed + self.in_flight + self.queued.len() < self.cfg.top_n.max(1)
        {
            *stopped |= ctx.take_daily_cancel();
            if *stopped {
                break;
            }
            let Some(card) = self.candidates.pop_front() else {
                if self.next_page > self.cfg.max_pages.max(1) || self.error.is_some() {
                    break;
                }
                match scraper::fetch_popular(&fetch, &self.cfg, self.next_page).await {
                    Ok(cards) => self.candidates.extend(cards),
                    Err(e) => {
                        self.error = Some(crate::util::cloudflare_hint(&format!("{e:#}")));
                    }
                }
                self.next_page += 1;
                continue;
            };
            if !self.seen.insert(card.id.clone()) {
                continue;
            }
            self.report.candidates += 1;
            if ctx.is_completed(&card.id)
                || ctx.task(&card.id).is_some_and(|task| !task.is_terminal())
            {
                self.report.skipped += 1;
                continue;
            }
            let mut task = TaskInfo::new(&card.id, &card.url);
            task.title = card.title.clone();
            task.source_url = self.cfg.popular_url(1);
            task.message = "waiting for a download slot".into();
            if ctx.register_task(task) {
                self.queued.push_back(card);
            } else {
                self.report.skipped += 1;
            }
        }
    }
}

/// Run scheduled work with the shared timer; active jobs are never overlapped.
pub async fn run(
    ctx: Arc<AppCtx>,
    schedule: tokio::sync::watch::Receiver<media_config::app::Config>,
) {
    let mut timer =
        media_runtime::schedule::Timer::new(schedule, media_runtime::schedule::Module::Jav);
    loop {
        ctx.set_scheduler(|s| {
            s.enabled = timer.config.enabled;
            s.daily_time = timer.config.daily_time.clone();
            s.next_run_at = timer.next.map(|next| next.to_rfc3339());
        });
        if timer.tick().await {
            if let Err(error) = run_daily(ctx.clone(), "scheduled").await {
                log::error!("scheduled JAV run failed: {error:#}");
            }
            timer.finished();
        }
    }
}

/// Follow the ranking until `top_n` new downloads complete or the page limit
/// is reached. Serialised against other daily runs by `ctx.daily_lock`.
pub async fn run_daily(ctx: Arc<AppCtx>, trigger: &str) -> anyhow::Result<DailyReport> {
    // Only one daily run at a time: a second trigger (the timer firing while a
    // manual run is still going, for instance) is dropped rather than queued.
    let _guard = match ctx.daily_lock.try_lock() {
        Ok(guard) => guard,
        Err(_) => {
            log::warn!("a daily job is already running — ignoring the {trigger} trigger");
            return Ok(DailyReport::default());
        }
    };
    ctx.take_daily_cancel();

    let cfg = ctx.config();
    let mut report = DailyReport::default();

    ctx.set_scheduler(|s| {
        s.running = true;
        s.last_result = format!("running ({trigger})");
    });
    log::info!("daily job started ({trigger})");

    // Try the current credentials first. Fetcher refreshes only after a
    // same-site challenge or rejection, including when no cookie is cached.
    let mut links: Vec<_> = cfg
        .listing_links()
        .iter()
        .map(|link| LinkQueue::new(cfg.for_link(link)))
        .collect();
    let target: usize = links.iter().map(|link| link.cfg.top_n.max(1)).sum();
    let limit = cfg.concurrent_videos.clamp(1, 8);
    let mut stopped = false;
    let mut set = JoinSet::new();
    let mut workers = HashMap::new();

    loop {
        stopped |= ctx.take_daily_cancel();
        for link in &mut links {
            link.fill(&ctx, &mut stopped).await;
        }
        stopped |= ctx.take_daily_cancel();
        while !stopped && set.len() < limit {
            let Some((index, link)) = links
                .iter_mut()
                .enumerate()
                .find(|(_, link)| !link.queued.is_empty())
            else {
                break;
            };
            let card = link.queued.pop_front().expect("queue is not empty");
            if !ctx
                .task(&card.id)
                .is_some_and(|t| t.state == TaskState::Queued)
            {
                stopped = true;
                break;
            }
            link.in_flight += 1;
            link.report.attempted += 1;
            let id = card.id.clone();
            let ctx = Arc::clone(&ctx);
            let worker = set.spawn(async move {
                download_video(ctx, card).await;
            });
            workers.insert(worker.id(), (index, id));
        }

        let Some(joined) = set.join_next_with_id().await else {
            break;
        };
        let worker_id = match &joined {
            Ok((id, ())) => *id,
            Err(error) => error.id(),
        };
        let (index, id) = workers.remove(&worker_id).expect("registered worker");
        let link = &mut links[index];
        link.in_flight -= 1;
        match joined {
            Ok(_) if ctx.is_completed(&id) => link.report.completed += 1,
            Ok(_) => {
                if ctx.task(&id).is_some_and(|task| {
                    matches!(task.state, TaskState::Paused | TaskState::Cancelled)
                }) {
                    stopped = true;
                } else {
                    link.report.failed += 1;
                }
            }
            Err(e) => {
                link.report.failed += 1;
                ctx.update_task(&id, |task| {
                    task.state = TaskState::Failed;
                    task.phase = "failed".into();
                    task.message = format!("download worker failed: {e}");
                });
                log::error!("download task panicked: {e}");
            }
        }
    }

    let mut summaries = Vec::new();
    for link in &links {
        // An individual pause/cancel also stops the daily run. Keep the rest
        // resumable rather than leaving tasks queued without a worker.
        for card in &link.queued {
            ctx.update_task(&card.id, |task| {
                if task.state == TaskState::Queued {
                    task.state = TaskState::Paused;
                    task.phase = "paused".into();
                    task.message = "daily run stopped; resume to download".into();
                }
            });
        }
        report.candidates += link.report.candidates;
        report.skipped += link.report.skipped;
        report.attempted += link.report.attempted;
        report.completed += link.report.completed;
        report.failed += link.report.failed;
        let mut summary = format!(
            "{}: {}/{} completed",
            link.cfg.popular_path,
            link.report.completed,
            link.cfg.top_n.max(1)
        );
        if let Some(error) = &link.error {
            summary.push_str(&format!("; listing failed: {error}"));
        } else if !stopped && link.report.completed < link.cfg.top_n.max(1) {
            summary.push_str(&format!(
                "; not enough eligible videos within {} listing page(s)",
                cfg.max_pages.max(1)
            ));
        }
        summaries.push(summary);
    }
    let mut summary = format!(
        "{trigger}: {}/{} completed, {} attempted, {} failed, {} skipped; {}",
        report.completed,
        target,
        report.attempted,
        report.failed,
        report.skipped,
        summaries.join("; ")
    );
    if stopped {
        summary.push_str("; stopped by user");
    }
    log::info!("daily job finished — {summary}");
    ctx.set_scheduler(|s| {
        s.running = false;
        s.last_run_at = Some(now_rfc3339());
        s.last_result = summary;
    });
    if report.completed == target {
        ctx.mark_daily_run(&Local::now().format("%Y-%m-%d").to_string())
            .await?;
    }

    Ok(report)
}

/// Ask every in-flight download to stop and clean up.
pub fn cancel_active(ctx: &AppCtx) {
    ctx.request_daily_cancel();
    for task in ctx.tasks() {
        if !task.is_terminal() {
            ctx.update_task(&task.id, |t| {
                t.state = TaskState::Cancelled;
                t.phase = "cancelled".into();
                t.message = "cancelled by user".into();
            });
        }
    }
}

/// Pause every in-flight download.
pub fn pause_active(ctx: &AppCtx) {
    ctx.request_daily_cancel();
    for task in ctx.tasks() {
        if matches!(task.state, TaskState::Running | TaskState::Queued) {
            ctx.update_task(&task.id, |t| {
                t.state = TaskState::Paused;
                t.phase = "paused".into();
                t.message = "paused by user".into();
            });
        }
    }
}

/// Re-run every paused or failed task.
pub async fn resume_all(ctx: Arc<AppCtx>) -> usize {
    let resumable: Vec<TaskInfo> = ctx
        .tasks()
        .into_iter()
        .filter(|t| matches!(t.state, TaskState::Paused | TaskState::Failed))
        .collect();
    let count = resumable.len();
    for task in resumable {
        resume_task(Arc::clone(&ctx), &task.id);
    }
    count
}

/// Start (or restart) the download for a known post id.
pub fn resume_task(ctx: Arc<AppCtx>, id: &str) -> bool {
    let Some(task) = ctx.task(id) else {
        return false;
    };
    if !matches!(
        task.state,
        TaskState::Paused | TaskState::Failed | TaskState::Cancelled
    ) {
        return false;
    }
    let card = scraper::VideoCard {
        id: task.id.clone(),
        url: task.url.clone(),
        title: task.title.clone(),
        image_url: String::new(),
        duration_secs: None,
        rank: None,
    };
    ctx.update_task(id, |t| {
        t.state = TaskState::Queued;
        t.phase = "queued".into();
        t.message = "resuming".into();
    });
    tokio::spawn(async move {
        download_video(ctx, card).await;
    });
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    #[test]
    fn parses_valid_times() {
        assert_eq!(parse_daily_time("03:30"), (3, 30));
        assert_eq!(parse_daily_time(" 23:59 "), (23, 59));
        assert_eq!(parse_daily_time("0:05"), (0, 5));
    }

    #[test]
    fn falls_back_on_garbage() {
        assert_eq!(parse_daily_time("tomorrow"), (3, 30));
        assert_eq!(parse_daily_time("25:00"), (3, 30));
        assert_eq!(parse_daily_time("12:99"), (3, 30));
    }

    #[test]
    fn next_occurrence_is_in_the_future() {
        let now = Local.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap();
        let next = next_occurrence(now, "03:30");
        assert_eq!(next.date_naive(), now.date_naive());
        assert_eq!(next.hour(), 3);
        assert!(next > now);
    }

    #[test]
    fn next_occurrence_rolls_to_tomorrow_when_passed() {
        let now = Local.with_ymd_and_hms(2026, 1, 1, 5, 0, 0).unwrap();
        let next = next_occurrence(now, "03:30");
        assert_eq!(
            next.date_naive(),
            now.date_naive() + ChronoDuration::days(1)
        );
        assert!(next > now);
    }
}
