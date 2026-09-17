use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Periodic,
    Daily,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Schedule {
    pub enabled: bool,
    pub mode: Mode,
    pub interval_secs: u64,
    pub daily_time: String,
    pub run_on_start: bool,
}

impl Default for Schedule {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: Mode::Periodic,
            interval_secs: 900,
            daily_time: "03:30".into(),
            run_on_start: false,
        }
    }
}

impl Schedule {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=604800).contains(&self.interval_secs),
            "Interval must be between 1 and 604800 seconds"
        );
        let parts: Vec<_> = self.daily_time.split(':').collect();
        anyhow::ensure!(
            parts.len() == 2
                && parts[0].len() == 2
                && parts[1].len() == 2
                && parts[0].parse::<u32>().is_ok_and(|h| h < 24)
                && parts[1].parse::<u32>().is_ok_and(|m| m < 60),
            "Daily time must use HH:MM"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Schedules {
    pub telegram: Schedule,
    pub jav: Schedule,
}
impl Default for Schedules {
    fn default() -> Self {
        Self {
            telegram: Schedule {
                run_on_start: true,
                ..Schedule::default()
            },
            jav: Schedule {
                mode: Mode::Daily,
                ..Schedule::default()
            },
        }
    }
}
