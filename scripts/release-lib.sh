#!/usr/bin/env bash
#
# Shared helpers for scripts/local-release.sh. Source this file; do not
# execute it. Modeled on quack-on-demand's release-lib.sh: version math and
# GitHub-release idempotency probes so every release step can no-op the work
# it detects is already done, making a failed release resumable by re-running.

# Repo root, derived from this file's own location (scripts/release-lib.sh).
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SCALA_VERSION="${SCALA_VERSION:-2.13}"
GH_REPO="starlake-ai/starlake"

# ---- preflight helpers ----------------------------------------------------
die() { echo "ERROR: $*" >&2; exit 1; }

require_cmd() { command -v "$1" >/dev/null 2>&1 || die "$1 not on PATH${2:+ ($2)}."; }

require_gh_auth() {
  require_cmd gh "needed to create the GitHub release"
  gh auth status >/dev/null 2>&1 || die "'gh auth login' first (needed to create the GitHub release)."
}

require_clean_tree() { # <dir> <name>
  [[ -z "$(git -C "$1" status --porcelain)" ]] || {
    echo "ERROR: working tree of $2 ($1) is dirty. Commit or stash before releasing." >&2
    git -C "$1" status --short >&2
    exit 1
  }
}

warn_if_not_master() { # <dir> <name>
  local branch
  branch="$(git -C "$1" rev-parse --abbrev-ref HEAD)"
  if [[ "$branch" != "master" && "$branch" != "main" ]]; then
    if [[ "${RELEASE_YES:-0}" == "1" ]]; then
      echo "WARN: releasing $2 from '$branch' (RELEASE_YES=1, continuing)." >&2
    else
      echo "WARN: releasing $2 from '$branch'. Continue? [y/N]" >&2
      read -r ans
      [[ "$ans" =~ ^[Yy]$ ]] || exit 1
    fi
  fi
}

# confirm <prompt>: honors RELEASE_YES=1 for resumed / non-interactive runs.
confirm() {
  [[ "${RELEASE_YES:-0}" == "1" ]] && return 0
  echo "$1 [y/N]"
  read -r ans
  [[ "$ans" =~ ^[Yy]$ ]]
}

# ---- version math ---------------------------------------------------------
read_version() { # <version.sbt path>
  sed -n 's/.*"\(.*\)".*/\1/p' "$1"
}

strip_snapshot() { echo "${1%-SNAPSHOT}"; }

# 1.5.16 -> 1.5.17-SNAPSHOT (patch bump, sbt-release's default).
next_snapshot() {
  local v; v="$(strip_snapshot "$1")"
  local a b c; IFS=. read -r a b c <<<"$v"
  echo "${a}.${b}.$((c + 1))-SNAPSHOT"
}

set_version() { # <version.sbt path> <new version>
  sed -i.bak -E "s|\"[^\"]+\"|\"$2\"|" "$1"
  rm "$1.bak"
}

# ---- GitHub release idempotency ------------------------------------------
tag_exists() { # <dir> <tag>
  git -C "$1" rev-parse -q --verify "refs/tags/$2" >/dev/null
}

release_exists() { # <tag>
  gh release view "$1" --repo "$GH_REPO" >/dev/null 2>&1
}

release_has_asset() { # <tag> <asset name>
  gh release view "$1" --repo "$GH_REPO" --json assets --jq '.assets[].name' 2>/dev/null | grep -qxF "$2"
}
