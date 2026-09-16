# syntax=docker/dockerfile:1
FROM rust:1.98-bookworm AS chef

# BoringSSL and SQLite are compiled from source; bindgen needs libclang.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake golang-go libclang-dev perl \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --version 0.1.78 --locked
WORKDIR /build

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
# Keep compiled dependencies in a layer, not a cache mount: CI exports this
# layer with mode=max and restores it even on a fresh runner.
RUN cargo chef cook --release --locked --recipe-path recipe.json --bin md-rs
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
COPY web ./web
RUN cargo build --release --locked --bin md-rs \
    && cp target/release/md-rs /usr/local/bin/md-rs

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates chromium curl ffmpeg fonts-noto-core tini tzdata \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/md-rs /usr/local/bin/md-rs
ENV MEDIA_HOST=0.0.0.0 MEDIA_PORT=8080 CHROME_PATH=/usr/bin/chromium RUST_LOG=info
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl --fail --silent --output /dev/null "http://127.0.0.1:${MEDIA_PORT}/" || exit 1
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/md-rs"]
