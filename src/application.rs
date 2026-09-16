//! Owns startup ordering, failure cleanup, and process shutdown.

pub(crate) async fn run() -> anyhow::Result<()> {
    std::fs::create_dir_all(media_config::DIRECTORY)?;
    let database = media_storage::Database::open(media_storage::DATABASE_FILE).await?;
    media_migration::run(&database).await?;
    let config = media_config::app::load_or_create()?;
    config.validate()?;
    let (settings, updates) = tokio::sync::watch::channel(config.clone());
    anyhow::ensure!(
        !config.host.trim().is_empty(),
        "config/app.yaml: host must not be empty"
    );
    anyhow::ensure!(
        config.port != 0,
        "config/app.yaml: port must be between 1 and 65535"
    );
    let listener = tokio::net::TcpListener::bind((config.host.as_str(), config.port)).await?;
    // Let the container health check follow the running listener, even if the
    // YAML file has been edited since startup.
    let mut health_address = listener.local_addr()?;
    if health_address.ip().is_unspecified() {
        health_address.set_ip(if health_address.is_ipv4() {
            std::net::Ipv4Addr::LOCALHOST.into()
        } else {
            std::net::Ipv6Addr::LOCALHOST.into()
        });
    }
    std::fs::write(
        std::env::temp_dir().join("md-rs-http-address"),
        health_address.to_string(),
    )?;
    let mut engines = vec![media_telegram::start(database.clone(), updates.clone()).await?];
    match media_jav::start(database, updates).await {
        Ok(engine) => engines.push(engine),
        Err(error) => {
            media_runtime::shutdown_all(engines).await;
            return Err(error);
        }
    };
    let app = crate::web::router(&engines).merge(crate::settings::router(settings));
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
