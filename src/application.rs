//! Owns startup ordering, failure cleanup, and process shutdown.

pub(crate) async fn run() -> anyhow::Result<()> {
    let host = std::env::var("MEDIA_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("MEDIA_PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()?;
    let listener = tokio::net::TcpListener::bind((host.as_str(), port)).await?;
    std::fs::create_dir_all(media_config::DIRECTORY)?;
    let database = media_storage::Database::open(media_storage::DATABASE_FILE).await?;
    media_migration::run(&database).await?;
    let mut engines = vec![media_telegram::start(database.clone()).await?];
    match media_jav::start(database).await {
        Ok(engine) => engines.push(engine),
        Err(error) => {
            media_runtime::shutdown_all(engines).await;
            return Err(error);
        }
    };
    let app = crate::web::router(&engines);
    log::info!("md-rs: http://{}", listener.local_addr()?);
    let result = tokio::select! {
        result = axum::serve(listener, app) => result,
        result = wait_for_shutdown() => result,
    };
    log::info!("Stopping downloaders; send another interrupt to force exit");
    tokio::select! {
        _ = media_runtime::shutdown_all(engines) => {},
        _ = wait_for_shutdown() => std::process::exit(130),
    }
    Ok(result?)
}

async fn wait_for_shutdown() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
