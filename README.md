# md-rs

[md-rs](https://github.com/msdx321/md-rs) combines Telegram and JAV downloading in one Rust application. The web UI is named **Media Downloader**.

- Shared dashboard, download controls, history, and settings.
- Telegram login, subscriptions, filters, and scan cursors in the browser.
- JAV scheduled downloads and browser-assisted cookie refresh on failure.
- YAML configuration and SQLite storage.

## Run with Docker

```sh
docker compose up -d --build
```

Open http://127.0.0.1:8080. Persistent data lives in `./data/`. Set `TZ` to your timezone before starting Compose.

To use a published image:

```sh
export MEDIA_IMAGE=docker.io/your-username/md-rs:latest
docker compose pull
docker compose up -d --no-build
```

Use `edge` for the default branch or `latest` for a stable release. Stop an older Compose deployment before starting the renamed `md-rs` project.

The web UI has no authentication. Keep it local or put it behind an authenticated reverse proxy.

## Run from source

Requires Rust 1.98+, Chromium, ffmpeg, CMake, Go, Perl, a C++ compiler, and libclang.

```sh
cargo run --release --bin md-rs
```

Use `MEDIA_HOST` and `MEDIA_PORT` to change the listener. Configure both modules in the web UI; configuration files live in `config/`.

## Import existing data

Place old Telegram `config.yaml` and `data.yaml` in `config/telegram/`, and the old JAV config in `config/jav/config.yaml`. With Docker, use `data/config/` instead. Migration runs at startup and preserves the originals. Keep Telegram's login session at `sessions/tmd.session` (`data/sessions/tmd.session` with Docker).

## Development and publishing

```sh
cargo fmt --all
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
```

CI checks the code and publishes native AMD64/ARM64 images to `docker.io/<DOCKER_HUB_USERNAME>/md-rs`. Configure the GitHub Actions secrets `DOCKER_HUB_USERNAME` and `DOCKER_HUB_ACCESS_TOKEN`. Branch builds produce `edge` on the default branch; `v*` release tags produce version tags and stable releases update `latest`. Pull requests never publish.
