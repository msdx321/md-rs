#!/usr/bin/env bash
set -euo pipefail

bash .github/scripts/in-build-env.sh cargo test --release --locked --workspace --all-targets --all-features
bash .github/scripts/in-build-env.sh cargo build --release --locked --bin md-rs
mkdir -p "$RUNNER_TEMP/release-output/usr/local/bin"
cp target/release/md-rs "$RUNNER_TEMP/release-output/usr/local/bin/md-rs"
