mod application;
mod configuration;
mod jav;
mod logging;
mod migration;
mod p91;
mod runtime;
mod settings;
mod storage;
mod telegram;
mod web;

#[cfg(test)]
mod test_support;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    logging::init();
    log::info!("Starting md-rs {}", env!("CARGO_PKG_VERSION"));
    match application::run().await {
        Ok(()) => {
            log::info!("Shutdown complete");
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            log::error!("Application failed: {error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}
