//! One timer policy shared by all download engines.
use crate::configuration::schedule::{Mode, Schedule};
use chrono::{DateTime, Duration as ChronoDuration, Local, TimeZone};
use tokio::sync::watch;

pub fn parse_daily_time(value: &str) -> (u32, u32) {
    let mut parts = value.trim().split(':');
    let hour = parts.next().and_then(|h| h.trim().parse::<u32>().ok());
    let minute = parts.next().and_then(|m| m.trim().parse::<u32>().ok());
    match (hour, minute) {
        (Some(h), Some(m)) if h < 24 && m < 60 && parts.next().is_none() => (h, m),
        _ => (3, 30),
    }
}

pub fn next_occurrence(now: DateTime<Local>, hhmm: &str) -> DateTime<Local> {
    let (hour, minute) = parse_daily_time(hhmm);
    // Skip nonexistent local times; choose the earlier occurrence on DST fallback.
    for days in 0..=2 {
        if let Some(next) = (now.date_naive() + ChronoDuration::days(days))
            .and_hms_opt(hour, minute, 0)
            .and_then(|naive| Local.from_local_datetime(&naive).earliest())
            && next > now
        {
            return next;
        }
    }
    now + ChronoDuration::days(1)
}

pub enum Module {
    Telegram,
    Jav,
    P91,
}

pub struct Timer {
    module: Module,
    updates: watch::Receiver<crate::configuration::app::Config>,
    pub config: Schedule,
    pub next: Option<DateTime<Local>>,
}
impl Timer {
    pub fn new(
        mut updates: watch::Receiver<crate::configuration::app::Config>,
        module: Module,
    ) -> Self {
        let config = match module {
            Module::Telegram => updates.borrow_and_update().schedules.telegram.clone(),
            Module::Jav => updates.borrow_and_update().schedules.jav.clone(),
            Module::P91 => updates.borrow_and_update().schedules.p91.clone(),
        };
        let immediate = config.enabled && config.run_on_start;
        let mut timer = Self {
            updates,
            module,
            config,
            next: None,
        };
        timer.finished();
        if immediate {
            timer.next = Some(Local::now());
        }
        timer
    }

    pub fn finished(&mut self) {
        let now = Local::now();
        self.next = self.config.enabled.then(|| match self.config.mode {
            Mode::Periodic => now + ChronoDuration::seconds(self.config.interval_secs as i64),
            Mode::Daily => next_occurrence(now, &self.config.daily_time),
        });
    }

    /// False means settings changed; true means a run is due. Cancellation-safe.
    pub async fn tick(&mut self) -> bool {
        let delay = self
            .next
            .map(|next| (next - Local::now()).to_std().unwrap_or_default());
        tokio::select! {
            biased;
            changed = self.updates.changed() => {
                if changed.is_err() { std::future::pending::<()>().await; }
                let common = self.updates.borrow_and_update();
                let config = match self.module { Module::Telegram => common.schedules.telegram.clone(), Module::Jav => common.schedules.jav.clone(), Module::P91 => common.schedules.p91.clone() };
                drop(common);
                if config != self.config { self.config = config; self.finished(); }
                false
            }
            _ = async { match delay {
                Some(delay) => tokio::time::sleep(delay).await,
                None => std::future::pending::<()>().await,
            }} => true,
        }
    }
}
