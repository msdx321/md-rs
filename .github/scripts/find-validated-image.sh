#!/usr/bin/env bash
set -euo pipefail

# Only the publish job writes this tag, after all validation jobs succeed.
# New release commits normally miss; this shortcut is mainly useful on reruns.
errors=$(mktemp)
trap 'rm -f "$errors"' EXIT
if ! manifest=$(docker buildx imagetools inspect "$IMAGE:validated-$COMMIT" --format '{{json .Manifest}}' 2> "$errors"); then
  if grep -Eq ': not found$|manifest unknown|MANIFEST_UNKNOWN' "$errors"; then
    echo "No previously published image for this commit (expected on a first release)." >&2
  else
    echo "::warning::Could not check the published image; running full validation and builds instead." >&2
    cat "$errors" >&2
  fi
  exit 0
fi

# Reject incomplete images or a tag pointing to a different commit/repository.
jq -r --arg commit "$COMMIT" --arg source "https://github.com/$GITHUB_REPOSITORY" '
  select(.annotations["org.opencontainers.image.revision"] == $commit) |
  select(.annotations["org.opencontainers.image.source"] == $source) |
  select(([.manifests[] | select(.platform.os == "linux") | .platform.architecture] | unique) == ["amd64", "arm64"]) |
  .digest | select(test("^sha256:[0-9a-f]{64}$"))
' <<< "$manifest"
