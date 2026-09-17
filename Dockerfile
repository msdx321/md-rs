# syntax=docker/dockerfile:1
FROM rust:1.98-bookworm AS build-env

# BoringSSL and SQLite are compiled from source; bindgen needs libclang.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake golang-go libclang-dev perl zstd \
    && rm -rf /var/lib/apt/lists/*
RUN rustup component add rustfmt clippy
RUN cargo install cargo-chef --version 0.1.78 --locked
WORKDIR /build

# Application build
# CI hashes the build environment above this marker and publishes it separately.
FROM build-env AS chef

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
# Local source builds cache dependencies separately from application sources.
RUN cargo chef cook --release --locked --recipe-path recipe.json --all-targets --all-features
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY web ./web
# Test and build in the same directory so release dependencies are reused.
RUN cargo test --release --locked --all-targets --all-features \
    && cargo build --release --locked --bin md-rs \
    && cp target/release/md-rs /usr/local/bin/md-rs

FROM debian:bookworm-slim AS runtime-base
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates chromium curl ffmpeg fonts-noto-core tini tzdata \
    && rm -rf /var/lib/apt/lists/*

FROM runtime-base AS runtime
# CI overrides the builder context with a tested binary, skipping Rust stages.
COPY --from=builder /usr/local/bin/md-rs /usr/local/bin/md-rs
ENV MEDIA_HOST=0.0.0.0 MEDIA_PORT=8080 CHROME_PATH=/usr/bin/chromium RUST_LOG=info
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl --fail --silent --output /dev/null "http://$(cat /tmp/md-rs-http-address)/" || exit 1
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/md-rs"]
