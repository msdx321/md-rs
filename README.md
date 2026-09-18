# md-rs

An all-in-one media downloader built with Rust, with a web UI for managing downloads, schedules, history, and settings.

## Install with Docker

Prebuilt images are available on [Docker Hub](https://hub.docker.com/r/msdx321/md-rs) for Linux AMD64 and ARM64. No source checkout or local build is needed.

Create a `compose.yaml`:

```yaml
services:
  md-rs:
    image: msdx321/md-rs:latest
    ports:
      - "127.0.0.1:8080:8080"
    environment:
      TZ: UTC
      RUST_LOG: warn,md_rs=info
    volumes:
      - ./data:/data
    shm_size: 1gb
    stop_grace_period: 2m
    restart: unless-stopped
```

Start the application:

```sh
docker compose pull
docker compose up -d
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080) and configure your downloads in the web UI. Configuration, sessions, history, and downloaded files are stored in `./data/` by default. Change `TZ` to your timezone for schedules.

> The web UI has no authentication. Keep it local or use an authenticated reverse proxy.

## Update

```sh
docker compose pull
docker compose up -d
```

Use `latest` for the latest stable release, or pin a [version tag](https://hub.docker.com/r/msdx321/md-rs/tags) for controlled upgrades.

## Logs

```sh
docker compose logs -f md-rs
```

Recent logs are also available in the web UI. For diagnostics, set `RUST_LOG` to `warn,md_rs=debug` and recreate the container with `docker compose up -d`.
