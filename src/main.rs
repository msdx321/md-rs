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

/// Every in-flight HLS segment parks a blocking thread on its decrypting writer
/// for the whole transfer — at most `concurrent_videos` (8) ×
/// `segment_concurrency` (32) = 256 of them, plus one per merging video.
/// `tokio::fs` draws on the same pool for every file operation in all three
/// modules, so the 512-thread default would leave file I/O queueing behind
/// segment writers at high settings.
const MAX_BLOCKING_THREADS: usize = 1024;

fn main() -> std::process::ExitCode {
    logging::init();
    log::info!("Starting md-rs {}", env!("CARGO_PKG_VERSION"));
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(MAX_BLOCKING_THREADS)
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            log::error!("Cannot start the async runtime: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    runtime.block_on(async {
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
    })
}
