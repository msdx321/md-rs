#!/usr/bin/env bash
set -euo pipefail

# Only the publish job writes this tag, after all validation jobs succeed.
if ! manifest=$(docker buildx imagetools inspect "$IMAGE:validated-$COMMIT" --format '{{json .Manifest}}'); then
  exit 0
fi

# Reject incomplete images or a tag pointing to a different commit/repository.
jq -r --arg commit "$COMMIT" --arg source "https://github.com/$GITHUB_REPOSITORY" '
  select(.annotations["org.opencontainers.image.revision"] == $commit) |
  select(.annotations["org.opencontainers.image.source"] == $source) |
  select(([.manifests[] | select(.platform.os == "linux") | .platform.architecture] | unique) == ["amd64", "arm64"]) |
  .digest | select(test("^sha256:[0-9a-f]{64}$"))
' <<< "$manifest"
