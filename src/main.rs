mod application;
mod configuration;
mod jav;
mod logging;
mod migration;
mod runtime;
mod settings;
mod storage;
mod telegram;
mod web;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init();
    application::run().await
}
