#!/usr/bin/env bash
set -euo pipefail

# Identical paths let rust-cache consume Cargo metadata produced in the container.
exec docker run --rm \
  --user "$(id -u):$(id -g)" \
  --volume "$GITHUB_WORKSPACE:$GITHUB_WORKSPACE" \
  --volume "$CARGO_HOME:$CARGO_HOME" \
  --workdir "$GITHUB_WORKSPACE" \
  --env HOME="$CARGO_HOME" \
  --env CARGO_HOME \
  --env CARGO_INCREMENTAL \
  --env CARGO_PROFILE_DEV_DEBUG \
  --env CARGO_PROFILE_TEST_DEBUG \
  "$BUILD_ENV_IMAGE" "$@"
