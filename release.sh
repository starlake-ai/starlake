#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Starlake Release Script
#
# Releases starlake-core and starlake-api in lockstep, then bumps all version
# references across repos to the next SNAPSHOT.
#
# The SBT release plugin handles: version.sbt, git tag, publish, push.
# This script handles: pre-flight version checks, coordinating both repos,
# and updating non-SBT files (versions.sh, .versions, Dockerfile, etc.).
#
# Usage:
#   ./release.sh                              # Full release + bump
#   ./release.sh --skip-release               # Skip Sonatype, just bump versions
#   ./release.sh --dry-run                    # Preview what would change
# ============================================================================

SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"

# Parse args
DRY_RUN=false
SKIP_RELEASE=false
while [[ "${1:-}" == --* ]]; do
  case "$1" in
    --dry-run)      DRY_RUN=true; shift ;;
    --skip-release) SKIP_RELEASE=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# --- Repo paths ---
API_DIR="${SL_API_DIR:-$HOME/git/starlake-api}"
UI_DIR="${SL_UI_DIR:-$HOME/git/starlake-ui2}"
STARLAKE_HOME="${STARLAKE_HOME:-$HOME/starlake}"
PROFILE="$HOME/.bash_profile"

# --- Helper: extract version from a version.sbt file ---
read_version() {
  sed -n 's/.*"\(.*\)".*/\1/p' "$1"
}

# ============================================================================
# Step 0: Pre-flight — verify all repos are on the same version
# ============================================================================
echo "============================================"
echo "Pre-flight: checking version alignment"
echo "============================================"

CORE_VERSION=$(read_version "$SCRIPT_DIR/version.sbt")
echo "  starlake-core:  $CORE_VERSION"

ERRORS=0
if [[ -f "$API_DIR/version.sbt" ]]; then
  API_VERSION=$(read_version "$API_DIR/version.sbt")
  echo "  starlake-api:   $API_VERSION"
  if [[ "$CORE_VERSION" != "$API_VERSION" ]]; then
    echo "  ERROR: starlake-api version ($API_VERSION) != starlake-core version ($CORE_VERSION)"
    ((ERRORS++))
  fi
else
  echo "  starlake-api:   (not found at $API_DIR)"
fi

# Check non-SBT files too
for file in "$STARLAKE_HOME/versions.sh" "$API_DIR/.versions" "$UI_DIR/.versions"; do
  if [[ -f "$file" ]]; then
    if ! grep -q "SL_VERSION.*$CORE_VERSION\|SL_VERSION=$CORE_VERSION" "$file" 2>/dev/null; then
      ACTUAL=$(grep "SL_VERSION" "$file" | head -1)
      echo "  WARNING: $file has $ACTUAL (expected $CORE_VERSION)"
    fi
  fi
done

if [[ "$ERRORS" -gt 0 ]]; then
  echo ""
  echo "Version mismatch detected. Fix before releasing."
  echo "To align starlake-api: update $API_DIR/version.sbt to match $CORE_VERSION"
  exit 1
fi

echo "  All versions aligned."
echo ""

# ============================================================================
# Step 1: Release to Sonatype (SBT handles version.sbt + git tag + push)
# ============================================================================
if [[ "$DRY_RUN" == false && "$SKIP_RELEASE" == false ]]; then

  # Helper: stash dirty files, release, then pop stash
  release_repo() {
    local repo_dir="$1"
    local repo_name="$2"
    cd "$repo_dir"

    local stashed=false
    if ! git diff --quiet HEAD 2>/dev/null; then
      echo "  Stashing uncommitted changes in $repo_name..."
      git stash --include-untracked
      stashed=true
    fi

    RELEASE_SONATYPE=true sbt 'release with-defaults'

    if [[ "$stashed" == true ]]; then
      echo "  Restoring stashed changes in $repo_name..."
      git stash pop || echo "  WARNING: stash pop had conflicts — resolve manually"
    fi
  }

  echo "============================================"
  echo "Step 1a: Release starlake-core $CORE_VERSION"
  echo "============================================"
  read -p "Proceed? [y/N]: " CONFIRM
  if [[ "$CONFIRM" =~ ^[Yy]$ ]]; then
    release_repo "$SCRIPT_DIR" "starlake-core"
    echo "starlake-core released."
  else
    echo "Skipped."
  fi

  if [[ -d "$API_DIR" ]]; then
    echo ""
    echo "============================================"
    echo "Step 1b: Release starlake-api $CORE_VERSION"
    echo "============================================"
    read -p "Proceed? [y/N]: " CONFIRM_API
    if [[ "$CONFIRM_API" =~ ^[Yy]$ ]]; then
      release_repo "$API_DIR" "starlake-api"
      echo "starlake-api released."
    else
      echo "Skipped."
    fi
    cd "$SCRIPT_DIR"
  fi

  echo ""
  echo "SBT release plugin has updated version.sbt in both repos."
  echo ""
fi

# ============================================================================
# Step 2: Read the new SNAPSHOT version (set by SBT release plugin)
#         and propagate to all non-SBT files
# ============================================================================
NEXT_VERSION=$(read_version "$SCRIPT_DIR/version.sbt")

echo "============================================"
echo "Step 2: Propagate $NEXT_VERSION to all config files"
echo "============================================"

# These files are NOT managed by SBT release plugin — we update them manually.
# version.sbt files are already updated by SBT, so they're excluded.
NON_SBT_FILES=(
  "$STARLAKE_HOME/versions.sh"
  "$API_DIR/.versions"
  "$API_DIR/versions.sh"
  "$UI_DIR/.versions"
  "$UI_DIR/Dockerfile"
  "$UI_DIR/.github/workflows/docker-hub-amd-arm.yml"
)

UPDATED=0
SKIPPED=0

for file in "${NON_SBT_FILES[@]}"; do
  if [[ -f "$file" ]]; then
    if grep -q "$CORE_VERSION" "$file"; then
      if [[ "$DRY_RUN" == true ]]; then
        echo "  [DRY-RUN] Would update: $file"
      else
        sed -i'' -e "s/$CORE_VERSION/$NEXT_VERSION/g" "$file"
        echo "  Updated: $file"
      fi
      ((UPDATED++))
    else
      echo "  Skipped (version not found): $file"
      ((SKIPPED++))
    fi
  else
    echo "  Skipped (file missing): $file"
    ((SKIPPED++))
  fi
done

echo ""
echo "Updated: $UPDATED files, Skipped: $SKIPPED"

if [[ "$DRY_RUN" == true ]]; then
  echo ""
  echo "[DRY-RUN] No files were modified."
  exit 0
fi

# --- Update LOCAL_STARLAKE_VERSION in bash profile ---
if [[ -f "$PROFILE" ]] && grep -q "LOCAL_STARLAKE_VERSION" "$PROFILE"; then
  sed -i'' -e "s/LOCAL_STARLAKE_VERSION=.*/LOCAL_STARLAKE_VERSION=$NEXT_VERSION/" "$PROFILE"
  echo "Updated LOCAL_STARLAKE_VERSION in $PROFILE"
fi

# ============================================================================
# Step 3: Rebuild
# ============================================================================
echo ""
echo "============================================"
echo "Step 3: Rebuild setup + assembly"
echo "============================================"
read -p "Run rebuild? [y/N]: " REBUILD
if [[ "$REBUILD" =~ ^[Yy]$ ]]; then
  cd "$SCRIPT_DIR"
  source "$PROFILE" 2>/dev/null || true
  sbt clean compile packageSetup
  if [[ -x "$SCRIPT_DIR/tmpsbt.sh" ]]; then
    "$SCRIPT_DIR/tmpsbt.sh"
  fi
  echo "Rebuild complete."
fi

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "============================================"
echo "Done! Released $CORE_VERSION, now on $NEXT_VERSION"
echo ""
echo "Remaining manual steps:"
echo "  1. Commit & push non-SBT file changes in starlake-api"
echo "  2. Commit & push non-SBT file changes in starlake-ui2"
echo "  3. Rebuild docker images"
echo "============================================"