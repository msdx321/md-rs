//! Web-driven API credential setup, separate from Telegram authorization.
use crate::{api, config};

pub(crate) async fn configure(
    state: &api::ApiState,
    replace: bool,
) -> anyhow::Result<config::Config> {
    if !replace
        && let Some(cfg) = config::FILE.load_optional()?
        && cfg.api_id > 0
        && cfg.api_hash.len() == 32
        && cfg.api_hash.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Ok(cfg);
    }
    let mut message = "Enter the API ID and API hash from your Telegram application.";
    loop {
        let input = state.login_prompt("credentials", message).await?;
        let api_id = input.value.trim().parse::<i32>().ok().filter(|id| *id > 0);
        let api_hash = input.api_hash.trim();
        if let Some(api_id) = api_id
            && api_hash.len() == 32
            && api_hash.bytes().all(|b| b.is_ascii_hexdigit())
        {
            let _edit = state.config_update.lock().await;
            let mut cfg = config::FILE.load_optional()?.unwrap_or_default();
            cfg.api_id = api_id;
            cfg.api_hash = api_hash.into();
            config::FILE.save(&cfg)?;
            return Ok(cfg);
        }
        message = "Use a positive numeric API ID and a 32-character hexadecimal API hash.";
    }
}
