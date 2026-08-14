# All artifacts on GitHub Releases: snapshots included, Sonatype retired

Date: 2026-08-14
Status: Approved

## Goal

Every starlake-core and starlake-api artifact, snapshot and release alike, is
distributed from GitHub Releases on `starlake-ai/starlake`. Sonatype publishing
is retired completely. starlake-ui stays Docker-only: it has no non-Docker
artifact today and gains none (decision).

## Current state (before)

- Releases (tag `vX.Y.Z`): core assembly jar + api universal zip + sha256s on
  GitHub Releases. Already done by the 2026-07-22 migration.
- Core SNAPSHOT jars: published to Sonatype `maven-snapshots` by the core
  `snapshot-jar-only` workflow (`sbt publish`). Consumed by
  `starlake-api/scripts/download-lib.sh` (snapshot branch) and core
  `Setup.java` (snapshot branch added 2026-08-12).
- API snapshots: `sbt publish` to Sonatype by the api `snapshot-jar-only`
  workflow. No consumer exists (nothing resolves the maven coordinates; the
  docker build compiles from source; Setup only downloads release zips).
- The core snapshot workflow also runs a vestigial `delete-old-packages`
  action against GitHub Packages.

## Decisions

- Snapshot hosting: a version-tagged pre-release `v<version>-SNAPSHOT` on
  `starlake-ai/starlake`, assets clobbered on each publish. URL shape is
  identical to releases: `releases/download/v<version>/<asset>`.
- The pre-release carries BOTH the core jar and the api zip (with sha256s),
  giving Setup full snapshot parity with releases.
- Sonatype: full retirement, no dual-publish period.
- UI: nothing to add.

## Part 1: Snapshot pre-release contract

Tag `v<version>-SNAPSHOT` (e.g. `v1.7.1-SNAPSHOT`) on `starlake-ai/starlake`,
marked `--prerelease` so it is never `latest`. Assets:

- `starlake-core_2.13-<v>-SNAPSHOT-assembly.jar` + `.sha256`
- `starlake-api_2.13-<v>-SNAPSHOT.zip` + `.sha256`

Rules:

- Created idempotently by whichever workflow publishes first
  (`gh release view || gh release create --prerelease`).
- Every publish overwrites its own assets (`gh release upload --clobber`).
- `scripts/local-release.sh` deletes the `vX.Y.Z-SNAPSHOT` pre-release AND its
  tag right after release `vX.Y.Z` is published (new housekeeping step). The
  next snapshot pre-release (`vX.Y.(Z+1)-SNAPSHOT`) appears lazily on the
  first snapshot publish after the bump.

## Part 2: Core workflow (`.github/workflows/snapshot-jar-only.yml`)

- Remove: the `SONATYPE_*` env step, the `delete-old-packages` step, and the
  `sbt ... publish` invocation.
- Add: `sbt assembly`, sha256 generation (`shasum -a 256`), idempotent
  pre-release creation, `gh release upload --clobber` of jar + sha256 using
  `GITHUB_TOKEN` (same repo).
- Keep: the Slack step, JDK/sbt setup, and the trigger-starlake-api step.

## Part 3: API workflow (`starlake-api/.github/workflows/snapshot-jar-only.yml`)

- Remove: the `SONATYPE_*` env step and `sbt ... publish`.
- Add: build the universal zip with the exact sbt invocation
  `scripts/local-release.sh` step 4 uses for the release zip, sha256,
  idempotent pre-release creation on `starlake-ai/starlake` and asset upload
  with `--clobber`, authenticated with the existing cross-repo
  `STARLAKE_API_TOKEN` secret.
- Keep: `download-lib.sh` step (it now pulls the core snapshot jar from the
  pre-release) and the `workflow_run` chain into `docker-hub-amd-arm`.
- Ordering note: the core workflow publishes the core jar BEFORE triggering
  the api workflow, so download-lib.sh always finds the jar.

## Part 4: Consumers collapse to one URL scheme

- `src/main/java/Setup.java` (core): delete the `SL_SNAPSHOT_BASE_URL`
  Sonatype branch added on 2026-08-12. `STARLAKE_RELEASE_JAR` and
  `SL_API_RELEASE_ZIP` both use
  `SL_RELEASE_BASE_URL + "/v" + version + "/..."` for snapshot and release
  alike. Rebuild and commit `distrib/setup.jar`.
- `starlake-api/scripts/download-lib.sh`: the snapshot/release if-else
  collapses to a single `curl -fL` against
  `https://github.com/starlake-ai/starlake/releases/download/v${SL_VERSION}/starlake-core_${SCALA_VERSION}-${SL_VERSION}-assembly.jar`.

## Part 5: Sonatype retirement

- Both workflows lose their `SONATYPE_*` env blocks.
- `starlake-api/build.sbt`: remove the sonatype publishing settings
  (`sonatypeProjectHosting`, `sonatypeProfileName`, `sonatypeCredentialHost`,
  the central-snapshots `publishTo`) and the sbt-sonatype plugin import if
  nothing else uses it.
- Core `build.sbt`: same treatment for its snapshot publishing settings.
- GitHub secrets `SONATYPE_USERNAME`/`SONATYPE_PASSWORD` become unused; left
  in place (deleting them is a manual owner action, out of scope).

## Part 6: Verification

- Dispatch core `snapshot-jar-only`; expect: pre-release `v1.7.1-SNAPSHOT`
  exists with core jar + sha256; api workflow triggered; api compile succeeds
  using the new download-lib URL; api zip + sha256 appear on the same
  pre-release; `docker-hub-amd-arm` builds and pushes
  `starlake-1.7-api:1.7.1-SNAPSHOT` using the new setup.jar.
- Anonymous `curl -fI` returns 200 for all four assets.
- CHANGELOG entry under the current SNAPSHOT section; the
  `project_github_releases_migration` memory updated.

## Error handling

- A snapshot publish for a version whose pre-release was deleted mid-flight
  simply recreates it (idempotent create).
- `curl -f` in download-lib.sh and Setup's existing failure path make a
  missing asset fail loudly instead of producing a corrupt jar.
- If the api workflow runs before the core one ever published the current
  SNAPSHOT version, download-lib.sh fails fast with a clear 404; the fix is
  running the core workflow first (documented in both workflow files).
