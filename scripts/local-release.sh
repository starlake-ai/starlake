#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Starlake Release Script
#
# Releases starlake-core and starlake-api in lockstep, then bumps all version
# references across repos to the next SNAPSHOT.
#
# Usage (from repo root):
#   ./scripts/local-release.sh                     # Run all steps
#   ./scripts/local-release.sh --steps 1,2,3       # Run specific steps only
#   ./scripts/local-release.sh --dry-run            # Preview what would change
#
# Steps:
#   1 - Release starlake-core + starlake-api to Sonatype
#   2 - Propagate next SNAPSHOT version to all config files
#   3 - Build and push setup.jar
#   4 - Build full assembly (tmpsbt.sh)
# ============================================================================

SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"
REPO_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"

# Ensure we're running from the repo root
if [[ "$(pwd)" != "$REPO_DIR" ]]; then
  echo "ERROR: Please run this script from the repo root: $REPO_DIR"
  echo "  cd $REPO_DIR && ./scripts/local-release.sh"
  exit 1
fi

# --- Repo paths ---
API_DIR="${SL_API_DIR:-$HOME/git/starlake-api}"
UI_DIR="${SL_UI_DIR:-$HOME/git/starlake-ui2}"
STARLAKE_HOME="${STARLAKE_HOME:-$HOME/starlake}"
PROFILE="$HOME/.bash_profile"

# --- Parse args ---
DRY_RUN=false
STEPS="1,2,3,4"
while [[ "${1:-}" == --* ]]; do
  case "$1" in
    --dry-run)  DRY_RUN=true; shift ;;
    --steps)    STEPS="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

should_run() {
  [[ "$STEPS" == *"$1"* ]]
}

# --- Helper: extract version from a version.sbt file ---
read_version() {
  sed -n 's/.*"\(.*\)".*/\1/p' "$1"
}

# --- Helper: stash dirty files, release, then pop stash ---
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

# ============================================================================
# Pre-flight — verify all repos are on the same version
# ============================================================================
echo "============================================"
echo "Pre-flight: checking version alignment"
echo "============================================"

CORE_VERSION="${CORE_VERSION:-$(read_version "$REPO_DIR/version.sbt")}"
echo "  starlake-core:  $CORE_VERSION"

ERRORS=0
if [[ -f "$API_DIR/version.sbt" ]]; then
  API_VERSION="${API_VERSION:-$(read_version "$API_DIR/version.sbt")}"
  echo "  starlake-api:   $API_VERSION"
  if [[ "$CORE_VERSION" != "$API_VERSION" ]]; then
    echo "  ERROR: starlake-api version ($API_VERSION) != starlake-core version ($CORE_VERSION)"
    ERRORS=$((ERRORS + 1))
  fi
else
  echo "  starlake-api:   (not found at $API_DIR)"
fi

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
  exit 1
fi
echo "  All versions aligned."

echo ""
echo "Steps to run: $STEPS"
echo "  1 - Release to Sonatype"
echo "  2 - Propagate next SNAPSHOT version"
echo "  3 - Build and push setup.jar"
echo "  4 - Build full assembly"
echo ""

# ============================================================================
# Step 1: Release to Sonatype
# ============================================================================
if should_run 1; then
  if [[ "$DRY_RUN" == true ]]; then
    echo "[DRY-RUN] Step 1: Would release starlake-core + starlake-api $CORE_VERSION"
  else
    echo "============================================"
    echo "Step 1a: Release starlake-core $CORE_VERSION"
    echo "============================================"
    release_repo "$REPO_DIR" "starlake-core"
    echo "starlake-core released."

    if [[ -d "$API_DIR" ]]; then
      echo ""
      echo "============================================"
      echo "Step 1b: Release starlake-api $CORE_VERSION"
      echo "============================================"
      release_repo "$API_DIR" "starlake-api"
      echo "starlake-api released."
      cd "$REPO_DIR"
    fi

    echo ""
    echo "SBT release plugin has updated version.sbt in both repos."
  fi
fi

# ============================================================================
# Step 2: Propagate next SNAPSHOT version to non-SBT files
# ============================================================================
if should_run 2; then
  # Read the new version (set by SBT release plugin in step 1, or current if step 1 skipped)
  NEXT_VERSION=$(read_version "$REPO_DIR/version.sbt")

  echo ""
  echo "============================================"
  echo "Step 2: Propagate $NEXT_VERSION to config files"
  echo "============================================"

  # Files where version appears on SL_VERSION lines
  SL_VERSION_FILES=(
    "$STARLAKE_HOME/versions.sh"
    "$API_DIR/.versions"
    "$API_DIR/versions.sh"
    "$UI_DIR/.versions"
  )

  # Files where version appears in other contexts (e.g. BASE_IMAGE_PATH, docker tags)
  BROAD_VERSION_FILES=(
    "$UI_DIR/Dockerfile"
    "$UI_DIR/.github/workflows/docker-hub-amd-arm.yml"
  )

  # Version pattern: matches X.Y.Z or X.Y.Z-SNAPSHOT
  VER_RE='[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\(-SNAPSHOT\)*'

  UPDATED=0
  SKIPPED=0

  # Update SL_VERSION lines
  for file in "${SL_VERSION_FILES[@]}"; do
    if [[ -f "$file" ]]; then
      if grep -q "SL_VERSION" "$file"; then
        if [[ "$DRY_RUN" == true ]]; then
          echo "  [DRY-RUN] Would update: $file"
          grep "SL_VERSION" "$file" | head -2 | while read -r line; do echo "    $line"; done
        else
          sed -i'' -e "/SL_VERSION/s/$VER_RE/$NEXT_VERSION/g" "$file"
          echo "  Updated: $file"
        fi
        UPDATED=$((UPDATED + 1))
      else
        echo "  Skipped (no SL_VERSION): $file"
        SKIPPED=$((SKIPPED + 1))
      fi
    else
      echo "  Skipped (file missing): $file"
      SKIPPED=$((SKIPPED + 1))
    fi
  done

  # Update files with broader version references (Dockerfile, GitHub workflows)
  for file in "${BROAD_VERSION_FILES[@]}"; do
    if [[ -f "$file" ]]; then
      if grep -q "starlake.*$VER_RE" "$file" 2>/dev/null || grep -q "SL_VERSION" "$file" 2>/dev/null; then
        if [[ "$DRY_RUN" == true ]]; then
          echo "  [DRY-RUN] Would update: $file"
          grep -n "$VER_RE" "$file" | head -5 | while read -r line; do echo "    $line"; done
        else
          sed -i'' -e "s/$VER_RE/$NEXT_VERSION/g" "$file"
          echo "  Updated: $file"
        fi
        UPDATED=$((UPDATED + 1))
      else
        echo "  Skipped (no version found): $file"
        SKIPPED=$((SKIPPED + 1))
      fi
    else
      echo "  Skipped (file missing): $file"
      SKIPPED=$((SKIPPED + 1))
    fi
  done

  echo "  Updated: $UPDATED files, Skipped: $SKIPPED"

  if [[ "$DRY_RUN" == false ]]; then
    if [[ -f "$PROFILE" ]] && grep -q "LOCAL_STARLAKE_VERSION" "$PROFILE"; then
      sed -i'' -e "s/LOCAL_STARLAKE_VERSION=.*/LOCAL_STARLAKE_VERSION=$NEXT_VERSION/" "$PROFILE"
      echo "  Updated LOCAL_STARLAKE_VERSION in $PROFILE"
    fi
  fi
fi

# ============================================================================
# Step 3: Build and push setup.jar
# ============================================================================
if should_run 3; then
  NEXT_VERSION=$(read_version "$REPO_DIR/version.sbt")

  echo ""
  echo "============================================"
  echo "Step 3: Build and push setup.jar"
  echo "============================================"

  if [[ "$DRY_RUN" == true ]]; then
    echo "[DRY-RUN] Would run: sbt clean compile packageSetup + git push distrib/setup.jar"
  else
    cd "$REPO_DIR"
    source "$PROFILE" 2>/dev/null || true
    sbt clean compile packageSetup

    if [[ -f "$REPO_DIR/distrib/setup.jar" ]]; then
      echo "Pushing updated setup.jar to GitHub..."
      git add distrib/setup.jar
      git commit -m "Update setup.jar for $NEXT_VERSION" || echo "  setup.jar unchanged, nothing to commit."
      git push origin master
      echo "setup.jar pushed."
    fi
  fi
fi

# ============================================================================
# Step 4: Build full assembly
# ============================================================================
if should_run 4; then
  echo ""
  echo "============================================"
  echo "Step 4: Build full assembly"
  echo "============================================"

  if [[ "$DRY_RUN" == true ]]; then
    echo "[DRY-RUN] Would run: tmpsbt.sh"
  else
    cd "$REPO_DIR"
    source "$PROFILE" 2>/dev/null || true
    if [[ -x "$REPO_DIR/tmpsbt.sh" ]]; then
      "$REPO_DIR/tmpsbt.sh"
      echo "Assembly complete."
    else
      echo "tmpsbt.sh not found or not executable."
    fi
  fi
fi

# ============================================================================
# Summary
# ============================================================================
NEXT_VERSION=$(read_version "$REPO_DIR/version.sbt")
echo ""
echo "============================================"
echo "Done! Now on $NEXT_VERSION"
echo ""
echo "Remaining manual steps:"
echo "  - Commit & push non-SBT file changes in starlake-api"
echo "  - Commit & push non-SBT file changes in starlake-ui2"
echo "  - Rebuild docker images"
echo "============================================"