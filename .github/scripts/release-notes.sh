#!/usr/bin/env bash
set -euo pipefail

tag=${1:?release tag required}
commit=$(git rev-parse "$tag^{commit}")
previous=$(git describe --tags --abbrev=0 --match 'v[0-9]*' "$commit^" 2>/dev/null || true)
repository="${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:?repository required}"
range=$commit
if [[ -n "$previous" ]]; then range="$previous..$commit"; fi

printf '## Changes\n\n'
# Include direct commits as well as merged PRs; omit version-only bookkeeping.
git log --reverse --no-merges --invert-grep --grep='^chore(release):' \
  --format="- %s ([%h]($repository/commit/%H))" "$range"
if [[ -n "$previous" ]]; then
  printf '\n**Full changelog:** [%s...%s](%s/compare/%s...%s)\n' \
    "$previous" "$tag" "$repository" "$previous" "$tag"
fi
