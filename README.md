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

Docker creates this file under `data/config/` with `host: 0.0.0.0`. On first start only, `MEDIA_HOST` and `MEDIA_PORT` seed the file; existing YAML takes precedence. Configure Telegram and JAV in the web UI. YAML fields are arranged in logical groups, sorted alphabetically within each group, and stripped of unknown fields on load and save. Provider files omit default values; list order is preserved.

History and download IDs expire after `history_retention_days` for both modules. Cleanup runs at startup, when settings change, and every minute. Downloaded files are kept; expired IDs no longer prevent downloads.

## Import existing data

Place old Telegram `config.yaml` and `data.yaml` in `config/telegram/`, and the old JAV config in `config/jav/config.yaml`. With Docker, use `data/config/` instead. Migration runs at startup and preserves the originals. Keep Telegram's login session at `sessions/tmd.session` (`data/sessions/tmd.session` with Docker).

## Development and publishing

```sh
cargo fmt --all
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
```

CI checks branches and pull requests. Pushes to `main` or `master` also prepare native AMD64/ARM64 images at `docker.io/<DOCKER_HUB_USERNAME>/md-rs:validated-<full-commit-sha>`, after Rust checks and both image builds pass. A `v*` tag promotes that exact commit's validated image by digest; if none exists, CI builds and validates it first. Stable release tags update `latest`. Branch and tag runs for the same commit are serialized to avoid duplicate builds. Configure the GitHub Actions secrets `DOCKER_HUB_USERNAME` and `DOCKER_HUB_ACCESS_TOKEN`.

Checks and release compilation use a shared build environment stored in `ghcr.io/<owner>/<repository>/build-env`. Its tag hashes the Dockerfile section above `# Application build`, plus the architecture. CI publishes missing environments using `GITHUB_TOKEN`; enable package writes for that token. PRs only read published environments and build locally when one is unavailable. Change that Dockerfile section to update the toolchain or build dependencies. Local `docker compose build` remains self-contained.

CI compiles and tests each release binary outside Docker's image build, in that shared environment. A separate cache of tested binaries includes the architecture, environment definition, Rust sources, embedded web assets, Cargo files, and release build scripts. An exact hit skips release compilation and tests, so documentation and runtime-packaging changes can reuse the binary. Otherwise Cargo restores compiled artifacts before testing and building. Image packaging copies the finished executable through the `builder` named context and never compiles Rust. Runtime layers use registry caches at `buildcache-amd64` and `buildcache-arm64`. Only pushes to `main` or `master` save Cargo and tested-binary caches; PR checks retain debug assertions and omit debug symbols.

Every Monday at 04:23 UTC, CI refreshes the default branch's runtime packages while reusing its validated binary, if available. The refreshed image becomes the validated candidate for that commit; existing release tags are unchanged. The workflow's manual `refresh_runtime` option performs the same refresh on the default branch. Ordinary builds retain cached runtime packages.
