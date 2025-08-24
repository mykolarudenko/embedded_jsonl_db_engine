#!/usr/bin/env bash
# Simple release helper for embedded_jsonl_db_engine
# Usage:
#   scripts/release.sh 0.1.0a3
# Actions:
#   - validate clean git state (except untracked ignored files)
#   - bump version in pyproject.toml (works from any subdir)
#   - commit with conventional message
#   - create and push tag v<version>
#   - push main branch
#
# Notes:
#   - Requires: bash, git, sed, grep, awk
#   - Does NOT touch .env or any secrets.
#   - Prints clear hints if something is missing.

set -euo pipefail

fail() { echo "[release] ERROR: $*" >&2; exit 1; }
info() { echo "[release] $*"; }

if [[ $# -ne 1 ]]; then
  fail "Please provide version argument, e.g.: scripts/release.sh 0.1.0a3"
fi

VERSION="$1"
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([abrc][0-9]+)?$ ]]; then
  fail "Version must look like 1.2.3 or 1.2.3a1/b1/rc1. Got: $VERSION"
fi

# Ensure we are on a git repo and determine repo root
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || fail "Not inside a git repository"
REPO_ROOT="$(git rev-parse --show-toplevel)"

# Work from repo root regardless of current dir
cd "$REPO_ROOT"

# Check that required files exist
[[ -f "pyproject.toml" ]] || fail "pyproject.toml not found in repo root: $REPO_ROOT"
[[ -f ".github/workflows/publish.yml" ]] || fail ".github/workflows/publish.yml not found"

# Ensure working tree is clean (ignoring untracked files)
if ! git diff --quiet --ignore-submodules --; then
  fail "Working tree has unstaged changes. Commit or stash them before releasing."
fi
if ! git diff --cached --quiet --ignore-submodules --; then
  fail "Index has staged changes. Commit or reset them before releasing."
fi

# Bump version in pyproject.toml
if ! grep -qE '^version = "' pyproject.toml; then
  fail 'Cannot find version = "..." in pyproject.toml'
fi

# Use awk replacement for the version line (first occurrence)
tmp_file="$(mktemp)"
awk -v newv="$VERSION" '
  BEGIN { done=0 }
  {
    if (!done && $0 ~ /^version = "[^"]*"/) {
      sub(/version = "[^"]*"/, "version = \"" newv "\"")
      done=1
    }
    print
  }
' pyproject.toml > "$tmp_file"
mv "$tmp_file" pyproject.toml

info "Bumped version in pyproject.toml to $VERSION"

# Commit and tag
git add pyproject.toml
git commit -m "chore(release): bump version to $VERSION"

# Push branch first (so tag has a referenced commit on remote)
current_branch="$(git rev-parse --abbrev-ref HEAD)"
info "Pushing branch $current_branch"
git push -u origin "$current_branch"

# Create and push tag
tag="v$VERSION"
git tag "$tag" -m "Release $tag"
info "Pushing tag $tag (this will trigger PyPI publish workflow)"
git push origin "$tag"

info "Done. Check GitHub Actions for publish status."
