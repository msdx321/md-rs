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

Open http://127.0.0.1:8080. Persistent data lives in `./data/`. Set `MEDIA_HOST`, `MEDIA_PORT`, and `TZ` in your environment or a `.env` file to change the published address, port, and timezone. The shared listener settings are in `data/config/app.yaml`; if you change its port, match `MEDIA_PORT` in the Compose mapping.

To use a published image:

```sh
export MEDIA_IMAGE=docker.io/your-username/md-rs:latest
docker compose pull
docker compose up -d --no-build
```

Use a version tag or `latest` for a stable release. Stop an older Compose deployment before starting the renamed `md-rs` project.

The web UI has no authentication. Keep it local or put it behind an authenticated reverse proxy.

## Run from source

Requires Rust 1.98+, Chromium, ffmpeg, CMake, Go, Perl, a C++ compiler, and libclang.

```sh
cargo run --release --bin md-rs
```

Configure shared storage, separate Telegram/JAV schedules, and the web listener in the Settings page. Listener changes require a restart. Common settings live in `config/app.yaml`:

```yaml
host: 127.0.0.1
port: 8080
telegram_download_path: downloads/telegram
jav_download_path: downloads/jav
temp_path: temp
history_retention_days: 30
```

Docker creates this file under `data/config/` with `host: 0.0.0.0`. On first start only, `MEDIA_HOST` and `MEDIA_PORT` seed the file; existing YAML takes precedence. Configure Telegram and JAV in the web UI. YAML keys are sorted and unknown fields removed on load and save. Provider files omit default values; list order is preserved.

History and download IDs expire after `history_retention_days` for both modules. Cleanup runs at startup, when settings change, and every minute. Downloaded files are kept; expired IDs no longer prevent downloads.

## Import existing data

Place old Telegram `config.yaml` and `data.yaml` in `config/telegram/`, and the old JAV config in `config/jav/config.yaml`. With Docker, use `data/config/` instead. Migration runs at startup and preserves the originals. Keep Telegram's login session at `sessions/tmd.session` (`data/sessions/tmd.session` with Docker).

## Development and publishing

```sh
cargo fmt --all
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
```

CI checks branches and pull requests. Only pushes of `v*` release tags build and publish native AMD64/ARM64 images to `docker.io/<DOCKER_HUB_USERNAME>/md-rs`; stable releases update `latest`. Configure the GitHub Actions secrets `DOCKER_HUB_USERNAME` and `DOCKER_HUB_ACCESS_TOKEN`.
