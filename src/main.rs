mod application;
mod web;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .filter_module("grammers_mtsender", log::LevelFilter::Warn)
        .filter_module("grammers_mtproto", log::LevelFilter::Warn)
        .filter_module("turso_core", log::LevelFilter::Warn)
        .init();
    application::run().await
}
