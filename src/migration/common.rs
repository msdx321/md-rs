//! Compatibility for settings formerly owned by individual providers.
use serde_yaml::Value;

pub(super) fn run() -> anyhow::Result<()> {
    let original: Value = if std::path::Path::new("config/app.yaml").exists() {
        crate::configuration::load("config/app.yaml")?
    } else {
        Value::Null
    };
    let mut common = if original.is_null() {
        crate::configuration::app::load_or_create()?
    } else {
        // Do not normalize until all legacy values have been imported successfully.
        serde_yaml::from_value(original.clone())?
    };
    for (name, file) in [
        ("telegram", crate::configuration::TELEGRAM_FILE),
        ("jav", crate::configuration::JAV_FILE),
    ] {
        if !std::path::Path::new(file).exists() {
            continue;
        }
        let old: Value = crate::configuration::load(file)?;
        let key = format!("{name}_download_path");
        if original.get(&key).is_none()
            && let Some(path) = old.get("save_path").and_then(Value::as_str)
        {
            if name == "telegram" {
                common.telegram_download_path = path.into();
            } else {
                common.jav_download_path = path.into();
            }
        }
        if name == "jav"
            && original.get("temp_path").is_none()
            && let Some(path) = old.get("temp_path").and_then(Value::as_str)
        {
            common.temp_path = path.into();
        }
        if original
            .get("schedules")
            .and_then(|s| s.get(name))
            .is_none()
        {
            if name == "telegram" {
                if let Some(interval) = old.get("check_interval_secs").and_then(Value::as_u64) {
                    common.schedules.telegram.interval_secs = interval;
                }
            } else {
                if let Some(enabled) = old.get("daily_enabled").and_then(Value::as_bool) {
                    common.schedules.jav.enabled = enabled;
                }
                if let Some(time) = old.get("daily_time").and_then(Value::as_str) {
                    common.schedules.jav.daily_time = time.into();
                }
                if let Some(start) = old.get("run_on_start").and_then(Value::as_bool) {
                    common.schedules.jav.run_on_start = start;
                }
            }
        }
    }
    common.validate()?;
    crate::configuration::app::FILE.save(&common)?;
    crate::configuration::telegram::FILE.load_optional()?;
    crate::configuration::jav::FILE.load_optional()?;
    Ok(())
}
