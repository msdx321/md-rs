//! The scheduled job: walk the configured listings in their own order, skip
//! what is already downloaded, and download the remainder.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use chrono::Local;
use serde::Serialize;
use tokio::task::JoinSet;

use crate::p91::app::{AppCtx, TaskInfo, TaskState, now_rfc3339};
use crate::p91::config::Config;
use crate::p91::downloader::{download_video, download_video_with};
use crate::p91::source::{http::Fetcher, scraper};

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
    fetch: Fetcher,
    report: DailyReport,
    candidates: VecDeque<scraper::VideoCard>,
    queued: VecDeque<scraper::VideoCard>,
    seen: HashSet<String>,
    next_page: usize,
    in_flight: usize,
    error: Option<String>,
}

impl LinkQueue {
    fn new(cfg: Config, fetch: Fetcher) -> Self {
        Self {
            cfg,
            fetch,
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
        while !*stopped
            && self.report.completed + self.in_flight + self.queued.len() < self.cfg.top_n.max(1)
        {
            *stopped |= ctx.jobs.is_closed() || ctx.take_daily_cancel();
            if *stopped {
                break;
            }
            if let Err(error) = self.fetch.check_current() {
                self.error = Some(format!("{error:#}"));
                *stopped = true;
                break;
            }
            let Some(card) = self.candidates.pop_front() else {
                if self.next_page > self.cfg.max_pages.max(1) || self.error.is_some() {
                    break;
                }
                let listing = tokio::select! {
                    biased;
                    _ = ctx.jobs.cancelled() => { *stopped = true; break; }
                    result = scraper::fetch_popular(&self.fetch, &self.cfg, self.next_page) => result,
                };
                match listing {
                    Ok(cards) => {
                        // A listing request establishes the site session.
                        match ctx.session().mark_warm(&self.fetch) {
                            Ok(()) => self.candidates.extend(cards),
                            Err(error) => self.error = Some(format!("{error:#}")),
                        }
                    }
                    Err(error) => self.error = Some(format!("{error:#}")),
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
            task.vid = card.vid.clone();
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
    schedule: tokio::sync::watch::Receiver<crate::configuration::app::Config>,
) {
    let mut timer =
        crate::runtime::schedule::Timer::new(schedule, crate::runtime::schedule::Module::P91);
    loop {
        ctx.set_scheduler(|s| {
            s.enabled = timer.config.enabled;
            s.daily_time = timer.config.daily_time.clone();
            s.next_run_at = timer.next.map(|next| next.to_rfc3339());
        });
        let tick = tokio::select! {
            biased;
            _ = ctx.jobs.cancelled() => break,
            tick = timer.tick() => tick,
        };
        if tick {
            if let Err(error) = run_daily(ctx.clone(), "scheduled").await {
                log::error!("scheduled 91Porn run failed: {error:#}");
            }
            timer.finished();
        }
    }
}

/// Follow each listing until its quota of new downloads completes or the page
/// limit is reached. Serialised against other daily runs by `ctx.daily_lock`.
pub async fn run_daily(ctx: Arc<AppCtx>, trigger: &str) -> anyhow::Result<DailyReport> {
    let Some(_owner) = ctx.jobs.enter() else {
        return Ok(DailyReport::default());
    };
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

    let (cfg, fetch) = ctx.request_context();
    let mut report = DailyReport::default();

    ctx.set_scheduler(|s| {
        s.running = true;
        s.last_result = format!("running ({trigger})");
    });
    log::info!("91Porn daily job started ({trigger})");

    let mut links: Vec<_> = cfg
        .listing_links()
        .iter()
        .map(|link| LinkQueue::new(cfg.for_link(link), fetch.clone()))
        .collect();
    let target: usize = links.iter().map(|link| link.cfg.top_n.max(1)).sum();
    let limit = cfg.concurrent_videos.clamp(1, 8);
    let mut stopped = false;
    let mut set = JoinSet::new();
    let mut workers = HashMap::new();

    loop {
        stopped |= ctx.jobs.is_closed() || ctx.take_daily_cancel();
        for link in &mut links {
            link.fill(&ctx, &mut stopped).await;
        }
        stopped |= ctx.jobs.is_closed() || ctx.take_daily_cancel();
        while !stopped && set.len() < limit {
            let Some((index, link)) = links
                .iter_mut()
                .enumerate()
                .find(|(_, link)| !link.queued.is_empty())
            else {
                break;
            };
            let card = link.queued.pop_front().expect("queue is not empty");
            if ctx.task_state(&card.id) != Some(TaskState::Queued) {
                stopped = true;
                break;
            }
            link.in_flight += 1;
            link.report.attempted += 1;
            let id = card.id.clone();
            let ctx = Arc::clone(&ctx);
            let cfg = link.cfg.clone();
            let fetch = link.fetch.clone();
            let worker = set.spawn(async move {
                download_video_with(ctx, card, cfg, fetch).await;
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
                if matches!(
                    ctx.task_state(&id),
                    Some(TaskState::Paused | TaskState::Cancelled)
                ) {
                    stopped = true;
                } else {
                    link.report.failed += 1;
                }
            }
            Err(error) => {
                link.report.failed += 1;
                ctx.update_task(&id, |task| {
                    task.state = TaskState::Failed;
                    task.phase = "failed".into();
                    task.message = format!("download worker failed: {error}");
                });
                log::error!("download task panicked: {error}");
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
        summary.push_str("; stopped");
    }
    log::info!("91Porn daily job finished — {summary}");
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
pub fn cancel_active(ctx: &AppCtx) -> usize {
    ctx.request_daily_cancel();
    ctx.tasks()
        .into_iter()
        .filter(|task| {
            let accepted = ctx.stop_task(&task.id, true);
            !accepted && matches!(task.state, TaskState::Running | TaskState::Queued)
        })
        .count()
}

/// Pause every in-flight download.
pub fn pause_active(ctx: &AppCtx) -> usize {
    ctx.request_daily_cancel();
    ctx.tasks()
        .into_iter()
        .filter(|task| {
            let accepted = ctx.stop_task(&task.id, false);
            !accepted && matches!(task.state, TaskState::Running | TaskState::Queued)
        })
        .count()
}

/// Re-run every paused or failed task.
pub async fn resume_all(ctx: Arc<AppCtx>) -> usize {
    let resumable: Vec<TaskInfo> = ctx
        .tasks()
        .into_iter()
        .filter(|t| matches!(t.state, TaskState::Paused | TaskState::Failed))
        .collect();
    resumable
        .into_iter()
        .filter(|task| resume_task(Arc::clone(&ctx), &task.id))
        .count()
}

/// Start (or restart) the download for a known view key.
pub fn resume_task(ctx: Arc<AppCtx>, id: &str) -> bool {
    if ctx.is_downloading(id) {
        return false;
    }
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
        vid: task.vid.clone(),
        hd: false,
        original: false,
    };
    let mut resumed = task;
    resumed.state = TaskState::Queued;
    resumed.phase = "queued".into();
    resumed.message = "resuming".into();
    if !ctx.register_task(resumed) {
        return false;
    }
    let jobs_ctx = ctx.clone();
    jobs_ctx
        .jobs
        .spawn(async move {
            download_video(ctx, card).await;
        })
        .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::schedule::{next_occurrence, parse_daily_time};
    use chrono::TimeZone;

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
    }

    #[test]
    fn next_occurrence_is_in_the_future() {
        let now = Local.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap();
        let next = next_occurrence(now, "03:30");
        assert!(next > now);
    }
}
