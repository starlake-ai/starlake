# Snapshots on GitHub Releases Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish core and api SNAPSHOT artifacts to a version-tagged GitHub pre-release on starlake-ai/starlake, retire Sonatype publishing entirely, and collapse all consumers to one URL scheme.

**Architecture:** A pre-release tagged `v<version>-SNAPSHOT` carries `starlake-core_2.13-<v>-assembly.jar` and `starlake-api_2.13-<v>.zip` plus sha256s, assets clobbered on each publish. The core `snapshot-jar-only` workflow builds and uploads the jar then triggers the api workflow, which builds and uploads the zip cross-repo, then chains into the docker build. `Setup.java` and `download-lib.sh` use the single `releases/download/v<version>/` URL for snapshots and releases alike. `local-release.sh` deletes the stale SNAPSHOT pre-release when the version ships.

**Tech Stack:** GitHub Actions, gh CLI, sbt (assembly, Universal/packageBin), bash, Java (Setup.java).

**Spec:** `docs/superpowers/specs/2026-08-14-github-releases-snapshots-design.md`

## Global Constraints

- Snapshot pre-release tag shape: `v<version>-SNAPSHOT` (e.g. `v1.7.1-SNAPSHOT`), always created with `--prerelease`.
- Asset names exactly as releases use them: `starlake-core_2.13-<v>-assembly.jar`, `starlake-api_2.13-<v>.zip`, each with a `.sha256` companion (sha256 format: `shasum -a 256` output, filename included).
- Single URL scheme everywhere: `https://github.com/starlake-ai/starlake/releases/download/v${SL_VERSION}/<asset>`; no Sonatype URL survives anywhere in either repo.
- All curl downloads use `-f` so missing assets fail loudly.
- Clean break: no dual publishing, no deprecated fallbacks.
- Repos: core = `/Users/hayssams/git/public/starlake` (branch master), api = `/Users/hayssams/git/starlake-api` (branch main). Commit in each repo; push is required for workflow tasks to take effect.
- Never use an em dash in anything you write.
- `docs/` in the core repo is gitignored; use `git add -f` for files under `docs/superpowers/`.

---

### Task 1: Core snapshot workflow publishes the jar to a GitHub pre-release

**Files:**
- Modify: `/Users/hayssams/git/public/starlake/.github/workflows/snapshot-jar-only.yml` (full replacement below)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: pre-release `v<version>-SNAPSHOT` on starlake-ai/starlake containing `starlake-core_2.13-<v>-assembly.jar` + `.sha256`. Task 2's api workflow and Task 3's consumers rely on these exact asset names. The trigger-starlake-api step runs only after the upload succeeds.

- [ ] **Step 1: Replace the workflow file**

Write this exact content to `.github/workflows/snapshot-jar-only.yml`:

```yaml
name: snapshot-jar-only
# On-demand only. Builds the core assembly at the current SNAPSHOT version,
# publishes it to the v<version>-SNAPSHOT pre-release on this repo, then
# triggers the starlake-api snapshot workflow (which uploads the api zip to
# the same pre-release and chains into the api docker build).
on:
  workflow_dispatch:
permissions:
  contents: write
jobs:
  snapshot:
    runs-on: ubuntu-latest
    steps:
      - name: Slack event
        if: always() # Pick up events even if the job fails or is canceled.
        uses: 8398a7/action-slack@v3
        with:
          status: ${{ job.status }}
          fields: repo,message,commit,author,action,eventName,ref,workflow,job,took,pullRequest # selectable (default: repo,message)
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_STARLAKE_CORE_WEBHOOK_URL }} # required
      - uses: actions/checkout@v4
      - name: Set up Zulu 17
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: '17'
      - name: Setup sbt launcher
        uses: sbt/setup-sbt@v1
        with:
          sbt-runner-version: '1.11.5'
      - name: Build assembly
        env:
          SBT_OPTS: "-Xss8M -Xmx16g -XX:+UseG1GC"
        run: sbt update clean assembly
      - name: Publish snapshot pre-release
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          set -euo pipefail
          V=$(sed -n 's/.*"\(.*\)".*/\1/p' version.sbt)
          case "$V" in *-SNAPSHOT) ;; *) echo "version.sbt holds '$V', not a SNAPSHOT; refusing"; exit 1 ;; esac
          JAR="target/scala-2.13/starlake-core_2.13-${V}-assembly.jar"
          test -f "$JAR"
          shasum -a 256 "$JAR" | sed "s| .*target/scala-2.13/| |" > "${JAR}.sha256"
          TAG="v${V}"
          gh release view "$TAG" --repo starlake-ai/starlake >/dev/null 2>&1 \
            || gh release create "$TAG" --repo starlake-ai/starlake --prerelease \
                 --title "$TAG" --notes "Rolling snapshot build. Assets are overwritten on every snapshot publish."
          gh release upload "$TAG" "$JAR" "${JAR}.sha256" --repo starlake-ai/starlake --clobber
      - name: trigger-starlake-api
        if: ${{ success() }}
        uses: actions/github-script@v6
        with:
          github-token: ${{ secrets.STARLAKE_API_TOKEN }}
          script: |
            await github.rest.actions.createWorkflowDispatch({
            owner: 'starlake-ai',
            repo: 'starlake-api',
            workflow_id: 'snapshot-jar-only.yml',
            ref: 'main'})
```

- [ ] **Step 2: Validate the YAML parses**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/snapshot-jar-only.yml')); print('YAML_OK')"
```
Expected: `YAML_OK`

- [ ] **Step 3: Verify no Sonatype reference remains in core workflows**

```bash
grep -rin sonatype /Users/hayssams/git/public/starlake/.github/workflows/ || echo CLEAN
```
Expected: `CLEAN` (the other snapshot-docker workflows do not reference Sonatype; if this grep finds any hit, stop and report instead of widening scope).

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/snapshot-jar-only.yml
git commit -m "ci: publish core snapshot jar to GitHub pre-release, drop Sonatype"
```

---

### Task 2: API snapshot workflow uploads the zip cross-repo

**Files:**
- Modify: `/Users/hayssams/git/starlake-api/.github/workflows/snapshot-jar-only.yml` (full replacement below)

**Interfaces:**
- Consumes: the pre-release created by Task 1 (its `download-lib.sh` step downloads the core jar from it once Task 3 lands; ordering is guaranteed because the core workflow uploads before triggering this one).
- Produces: `starlake-api_2.13-<v>.zip` + `.sha256` on the same pre-release. The `workflow_run` trigger of `docker-hub-amd-arm` is unchanged and fires when this workflow completes.
- Requires: repo secret `SL_CORE_RELEASES_TOKEN` on starlake-ai/starlake-api, a PAT with contents:write on starlake-ai/starlake (Step 3, manual).

- [ ] **Step 1: Replace the workflow file**

Write this exact content to `.github/workflows/snapshot-jar-only.yml`:

```yaml
name: snapshot-jar-only
# Dispatched by the core repo's snapshot-jar-only AFTER it published the core
# snapshot jar to the v<version>-SNAPSHOT pre-release on starlake-ai/starlake.
# Builds the api universal zip and uploads it to that same pre-release, then
# docker-hub-amd-arm fires via its workflow_run trigger.
# If run standalone before the core workflow ever published the current
# SNAPSHOT version, download-lib.sh fails fast with a 404: run the core
# workflow first.
on:
  workflow_dispatch:
jobs:
  snapshot:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Zulu 17
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: '17'
      - name: Setup sbt launcher
        uses: sbt/setup-sbt@v1
        with:
          sbt-runner-version: '1.11.5'
      - name: Download unmanaged lib JARs
        run: ./scripts/download-lib.sh
      - name: Build universal zip
        env:
          SBT_OPTS: "-Xss8M -Xmx16g -XX:+UseG1GC"
        run: sbt update clean compile Universal/packageBin
      - name: Publish zip to the snapshot pre-release
        env:
          GH_TOKEN: ${{ secrets.SL_CORE_RELEASES_TOKEN }}
        run: |
          set -euo pipefail
          V=$(sed -n 's/.*"\(.*\)".*/\1/p' version.sbt)
          case "$V" in *-SNAPSHOT) ;; *) echo "version.sbt holds '$V', not a SNAPSHOT; refusing"; exit 1 ;; esac
          BUILT="target/universal/starlake-api-${V}.zip"
          test -f "$BUILT"
          ZIP="target/universal/starlake-api_2.13-${V}.zip"
          cp "$BUILT" "$ZIP"
          shasum -a 256 "$ZIP" | sed "s| .*target/universal/| |" > "${ZIP}.sha256"
          TAG="v${V}"
          gh release view "$TAG" --repo starlake-ai/starlake >/dev/null 2>&1 \
            || gh release create "$TAG" --repo starlake-ai/starlake --prerelease \
                 --title "$TAG" --notes "Rolling snapshot build. Assets are overwritten on every snapshot publish."
          gh release upload "$TAG" "$ZIP" "${ZIP}.sha256" --repo starlake-ai/starlake --clobber
```

- [ ] **Step 2: Validate the YAML parses**

```bash
python3 -c "import yaml; yaml.safe_load(open('/Users/hayssams/git/starlake-api/.github/workflows/snapshot-jar-only.yml')); print('YAML_OK')"
```
Expected: `YAML_OK`

- [ ] **Step 3: Create the cross-repo secret (MANUAL, requires the user)**

The api workflow needs a PAT with contents:write on starlake-ai/starlake stored as `SL_CORE_RELEASES_TOKEN` in the api repo. This cannot be automated because the PAT value is only known to the user. Ask the user to run (with the PAT value at hand):

```bash
gh secret set SL_CORE_RELEASES_TOKEN --repo starlake-ai/starlake-api
```

(The PAT behind the core repo's existing `STARLAKE_API_TOKEN` secret works if it has `repo` scope covering starlake-ai/starlake.) Report NEEDS_CONTEXT and pause if the user is unavailable; do not skip this step silently.

- [ ] **Step 4: Verify no Sonatype reference remains in api workflows**

```bash
grep -rin sonatype /Users/hayssams/git/starlake-api/.github/workflows/ || echo CLEAN
```
Expected: `CLEAN`

- [ ] **Step 5: Commit**

```bash
cd /Users/hayssams/git/starlake-api
git add .github/workflows/snapshot-jar-only.yml
git commit -m "ci: upload api snapshot zip to the core repo pre-release, drop Sonatype"
```

---

### Task 3: Consumers on one URL scheme

**Files:**
- Modify: `/Users/hayssams/git/public/starlake/src/main/java/Setup.java` (remove the Sonatype snapshot branch)
- Modify: `/Users/hayssams/git/public/starlake/distrib/setup.jar` (rebuilt)
- Modify: `/Users/hayssams/git/starlake-api/scripts/download-lib.sh` (collapse the if-else)

**Interfaces:**
- Consumes: the asset names and URL shape from Tasks 1-2.
- Produces: `Setup.java` and `download-lib.sh` fetch core artifacts from `https://github.com/starlake-ai/starlake/releases/download/v${VERSION}/...` for every version, snapshot or release.

- [ ] **Step 1: Simplify Setup.java**

Replace this block (added 2026-08-12):
```java
    // Releases live on GitHub Releases; SNAPSHOT core jars are only published to Sonatype's
    // snapshot repository (by the snapshot-jar-only workflow), so snapshot versions resolve there.
    private static final String SL_SNAPSHOT_BASE_URL = "https://central.sonatype.com/repository/maven-snapshots/ai/starlake";
    private static final ResourceDependency STARLAKE_RELEASE_JAR = new ResourceDependency("starlake-core",
            SL_VERSION.endsWith("-SNAPSHOT")
                    ? SL_SNAPSHOT_BASE_URL + "/starlake-core_" + SCALA_VERSION + "/" + SL_VERSION + "/starlake-core_" + SCALA_VERSION + "-" + SL_VERSION + "-assembly.jar"
                    : SL_RELEASE_BASE_URL + "/v" + SL_VERSION + "/starlake-core_" + SCALA_VERSION + "-" + SL_VERSION + "-assembly.jar");
```
with:
```java
    // Snapshots and releases both live on GitHub Releases: snapshots as rolling
    // v<version>-SNAPSHOT pre-releases, so one URL scheme covers everything.
    private static final ResourceDependency STARLAKE_RELEASE_JAR = new ResourceDependency("starlake-core", SL_RELEASE_BASE_URL + "/v" + SL_VERSION + "/starlake-core_" + SCALA_VERSION + "-" + SL_VERSION + "-assembly.jar");
```

- [ ] **Step 2: Rebuild setup.jar and verify no Sonatype reference remains in core sources**

```bash
cd /Users/hayssams/git/public/starlake
sbt compile packageSetup
grep -rin "central.sonatype\|maven-snapshots" src/main/java/ src/main/scala/ && echo FOUND || echo CLEAN
```
Expected: compile succeeds; `CLEAN`.

- [ ] **Step 3: Collapse download-lib.sh**

In `/Users/hayssams/git/starlake-api/scripts/download-lib.sh`, replace:
```bash
if [ ! -f "lib/starlake-core_${SCALA_VERSION}-${SL_VERSION}-assembly.jar" ]; then
  if [[ $SL_VERSION == *"SNAPSHOT" ]]; then
    rm -f lib/starlake-core_${SCALA_VERSION}-*
    curl -fL https://central.sonatype.com/repository/maven-snapshots/ai/starlake/starlake-core_${SCALA_VERSION}/${SL_VERSION}/starlake-core_${SCALA_VERSION}-${SL_VERSION}-assembly.jar -o lib/starlake-core_${SCALA_VERSION}-${SL_VERSION}-assembly.jar
  else
      # Released core jars live on GitHub Releases since the move off Maven Central (2026-07-22)
      rm -f lib/starlake-core_${SCALA_VERSION}-*
      curl -fL https://github.com/starlake-ai/starlake/releases/download/v${SL_VERSION}/starlake-core_${SCALA_VERSION}-${SL_VERSION}-assembly.jar -o lib/starlake-core_${SCALA_VERSION}-${SL_VERSION}-assembly.jar
  fi
fi
```
with:
```bash
if [ ! -f "lib/starlake-core_${SCALA_VERSION}-${SL_VERSION}-assembly.jar" ]; then
  # Snapshots (rolling v<version>-SNAPSHOT pre-releases) and releases both live on GitHub Releases
  rm -f lib/starlake-core_${SCALA_VERSION}-*
  curl -fL https://github.com/starlake-ai/starlake/releases/download/v${SL_VERSION}/starlake-core_${SCALA_VERSION}-${SL_VERSION}-assembly.jar -o lib/starlake-core_${SCALA_VERSION}-${SL_VERSION}-assembly.jar
fi
```

- [ ] **Step 4: Syntax-check and verify no Sonatype reference remains in api scripts**

```bash
bash -n /Users/hayssams/git/starlake-api/scripts/download-lib.sh && echo SYNTAX_OK
grep -rin "central.sonatype\|maven-snapshots" /Users/hayssams/git/starlake-api/scripts/ && echo FOUND || echo CLEAN
```
Expected: `SYNTAX_OK` then `CLEAN`.

- [ ] **Step 5: Commit both repos**

```bash
cd /Users/hayssams/git/public/starlake
git add src/main/java/Setup.java distrib/setup.jar
git commit -m "fix(setup): one GitHub Releases URL scheme for snapshot and release jars"
cd /Users/hayssams/git/starlake-api
git add scripts/download-lib.sh
git commit -m "Fetch core jars from GitHub Releases for snapshots too"
```

---

### Task 4: Remove Sonatype publishing configuration from both builds

**Files:**
- Modify: `/Users/hayssams/git/public/starlake/build.sbt` (lines around 222-249)
- Modify: `/Users/hayssams/git/public/starlake/project/plugins.sbt` (drop sbt-sonatype if present and unused)
- Modify: `/Users/hayssams/git/starlake-api/build.sbt` (lines around 74-100)
- Modify: `/Users/hayssams/git/starlake-api/project/plugins.sbt` (same)

**Interfaces:**
- Consumes: nothing.
- Produces: both builds compile with no sonatype settings; `publishLocal` still works in core (the release script uses it).

- [ ] **Step 1: Core build.sbt**

Remove exactly these settings (keep `publishMavenStyle`, `licenses`, `pgpPassphrase`, `publishLocal / checksums`, `releaseCrossBuild`, and everything else):
```scala
// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "ai.starlake"
```
```scala
sonatypeProjectHosting := Some(
  GitHubHosting("starlake-ai", "starlake", "hayssam.saleh@starlake.ai")
)
```
```scala
sonatypeCredentialHost := sonatypeCentralHost
```
```scala
ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}
```
Then remove `import xerial.sbt.Sonatype.*` (line 5) IF no other `sonatype`/`GitHubHosting`/`localStaging` symbol remains in the file (grep first); otherwise leave the import and report which symbol still needs it.

- [ ] **Step 2: Core plugins.sbt**

```bash
grep -n "sonatype" /Users/hayssams/git/public/starlake/project/plugins.sbt
```
If the only remaining consumer was the removed block, delete the `addSbtPlugin(... "sbt-sonatype" ...)` line. If `grep -rn "sonatype" build.sbt project/` still shows live usages afterwards, restore the plugin line and report.

- [ ] **Step 3: Core build validates**

```bash
cd /Users/hayssams/git/public/starlake && sbt "show version" 2>&1 | tail -3
```
Expected: prints `1.7.1-SNAPSHOT` (or current), no unresolved reference errors.

- [ ] **Step 4: API build.sbt and plugins.sbt, same treatment**

Remove from `/Users/hayssams/git/starlake-api/build.sbt`:
```scala
sonatypeProjectHosting := Some(
  GitHubHosting("starlake-ai", "starlake-api", "hayssam.saleh@starlake.ai")
)
```
```scala
sonatypeProfileName := "ai.starlake"
```
```scala
sonatypeCredentialHost := sonatypeCentralHost
```
```scala
publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}
```
Then remove `import xerial.sbt.Sonatype.*` (line 6) under the same grep-first condition, and the sbt-sonatype plugin line in `project/plugins.sbt` under the same condition as Step 2.

- [ ] **Step 5: API build validates**

```bash
cd /Users/hayssams/git/starlake-api && sbt "show version" 2>&1 | tail -3
```
Expected: prints the current version, no errors.

- [ ] **Step 6: Commit both repos**

```bash
cd /Users/hayssams/git/public/starlake
git add build.sbt project/plugins.sbt
git commit -m "build: remove Sonatype publishing configuration"
cd /Users/hayssams/git/starlake-api
git add build.sbt project/plugins.sbt
git commit -m "build: remove Sonatype publishing configuration"
```

---

### Task 5: Release script deletes the stale SNAPSHOT pre-release

**Files:**
- Modify: `/Users/hayssams/git/public/starlake/scripts/local-release.sh` (end of step 6)
- Modify: `/Users/hayssams/git/public/starlake/CHANGELOG.md` (current SNAPSHOT section)

**Interfaces:**
- Consumes: `release_exists` helper from `scripts/release-lib.sh`, `run` wrapper, `$GH_REPO`, `$RELEASE_VERSION` (all already defined in the script).
- Produces: after a release publishes, no `v<released-version>-SNAPSHOT` pre-release or tag remains.

- [ ] **Step 1: Add the cleanup to step 6**

At the end of the step 6 block in `local-release.sh` (after the release is published, inside `if should_run 6; then ... fi`), add:
```bash
  # The just-released version's rolling snapshot pre-release is now stale:
  # delete it and its tag (assets for X.Y.Z-SNAPSHOT stop making sense once
  # vX.Y.Z exists). The next snapshot pre-release appears lazily on the first
  # snapshot publish after the version bump.
  SNAP_TAG="v${RELEASE_VERSION}-SNAPSHOT"
  if release_exists "$SNAP_TAG"; then
    echo "  deleting stale snapshot pre-release $SNAP_TAG"
    run gh release delete "$SNAP_TAG" --repo "$GH_REPO" --yes --cleanup-tag
  fi
```

- [ ] **Step 2: Syntax check and dry-run**

```bash
bash -n scripts/local-release.sh && echo SYNTAX_OK
RELEASE_VERSION=1.7.1 NEXT_VERSION=1.7.2-SNAPSHOT ./scripts/local-release.sh --dry-run 2>&1 | tail -20
```
Expected: `SYNTAX_OK`; the dry run completes and shows no error (the cleanup only prints when the snapshot pre-release exists).

- [ ] **Step 3: CHANGELOG entry**

In the current `# 1.7.1-SNAPSHOT:` section of CHANGELOG.md (create the section header above `# 1.7.0:` if it does not exist yet), add under `__Improvement__:` (create that subheading if absent):
```markdown
- **All artifacts on GitHub Releases**: SNAPSHOT builds of starlake-core (assembly jar) and starlake-api (zip) are now published to a rolling pre-release tagged `v<version>-SNAPSHOT` on starlake-ai/starlake, with sha256 companions, replacing Sonatype snapshot publishing entirely. Setup and CI download scripts use one URL scheme for snapshots and releases. The release flow deletes the stale snapshot pre-release when the version ships.
```

- [ ] **Step 4: Commit**

```bash
cd /Users/hayssams/git/public/starlake
git add scripts/local-release.sh CHANGELOG.md
git commit -m "release: delete stale snapshot pre-release after publishing, changelog"
```

---

### Task 6: Push, end-to-end verification, memory update

**Files:**
- No new edits expected; pushes and dispatches only. Memory file: `/Users/hayssams/.claude/projects/-Users-hayssams-git-public-starlake/memory/project_github_releases_migration.md`

**Interfaces:**
- Consumes: everything above, plus the `SL_CORE_RELEASES_TOKEN` secret from Task 2 Step 3 (verify it exists before dispatching).

- [ ] **Step 1: Push both repos**

```bash
cd /Users/hayssams/git/public/starlake && git push
cd /Users/hayssams/git/starlake-api && git push
```

- [ ] **Step 2: Confirm the secret exists**

```bash
gh secret list --repo starlake-ai/starlake-api | grep SL_CORE_RELEASES_TOKEN
```
Expected: one line. If absent, stop and ask the user (Task 2 Step 3).

- [ ] **Step 3: Dispatch the chain and watch it**

```bash
gh workflow run snapshot-jar-only.yml --repo starlake-ai/starlake --ref master
```
Then poll (60s interval) until: the core run completes with success; the api `snapshot-jar-only` run it triggers completes with success; the api `docker-hub-amd-arm` run triggered by that completes with success.
Expected: all three succeed.

- [ ] **Step 4: Verify the pre-release and anonymous downloads**

```bash
gh release view v1.7.1-SNAPSHOT --repo starlake-ai/starlake --json isPrerelease,assets --jq '{pre: .isPrerelease, assets: [.assets[].name]}'
for a in starlake-core_2.13-1.7.1-SNAPSHOT-assembly.jar starlake-core_2.13-1.7.1-SNAPSHOT-assembly.jar.sha256 starlake-api_2.13-1.7.1-SNAPSHOT.zip starlake-api_2.13-1.7.1-SNAPSHOT.zip.sha256; do
  curl -sSIL -o /dev/null -w "$a: %{http_code}\n" "https://github.com/starlake-ai/starlake/releases/download/v1.7.1-SNAPSHOT/$a"
done
```
Expected: `pre: true`, all four asset names listed, four `200` lines.

- [ ] **Step 5: Update the migration memory**

In `project_github_releases_migration.md`, replace the sentence about snapshots going to Sonatype (the "core snapshot-jar-only (publishes core snapshot to Sonatype, on demand)" description) with: snapshots now publish to the rolling `v<version>-SNAPSHOT` pre-release on starlake-ai/starlake (core jar by the core workflow, api zip by the api workflow via `SL_CORE_RELEASES_TOKEN`); Sonatype publishing is fully retired as of 2026-08-14; `local-release.sh` deletes the stale snapshot pre-release at release time.

- [ ] **Step 6: Report**

Summarize: commits pushed per repo, workflow run conclusions, asset list, download check results.
