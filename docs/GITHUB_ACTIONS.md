# GitHub Actions Workflows

This document describes the CI/CD workflows in `.github/workflows/`. All workflows run on `ubuntu-latest`.

## Summary

| Workflow | File | Trigger | Purpose |
|---|---|---|---|
| Build | `build.yml` | manual | Full test + coverage + Docker smoke test |
| test-only | `test-only.yml` | manual, PR to `master`/`branch-*` | Run tests on PRs |
| Test BQ Transform | `test-bq-transform.yml` | manual | Targeted BigQuery transform tests |
| snapshot-jar-only | `snapshot-jar-only.yml` | manual | Publish `-SNAPSHOT` jar to the `v<version>-SNAPSHOT` GitHub pre-release, then trigger `starlake-api` |
| snapshot-docker-no-tests | `snapshot-docker-no-tests.yml` | manual, after `snapshot-jar-only` succeeds | Publish snapshot + multi-arch Docker image (no tests) |
| snapshot-docker (with tests) | `snapshot-docker-with-tests.yml` | manual | Test + publish snapshot + Docker image |
| Release | `release.yml` | manual, tag `v*.*.*` (excluding `v*-SNAPSHOT`) | GitHub-release Docker build: multi-arch Docker push + dispatch the api image build |
| release-docker | `release-docker.yml` | manual | Build Docker from latest Maven Central release (no rebuild) |
| Scala Steward | `scala-steward.yml` | manual, weekly (Sun 00:00 UTC) | Open PRs for dependency updates |
| Slack Notification | `slack.yml` | push, PR, issue, comment, branch create/delete | Post repo activity to Slack |

## Workflow details

### `build.yml` — Build
**Trigger:** `workflow_dispatch` only.
**Jobs:**
- `test` — Sets up JDK 17 (Zulu), SBT, Python 3.12 + PySpark 4.1.3, graphviz, GCP auth. Runs `sbt ++2.13.14! coverage coverageReport` against Scala 2.13.14 with `SL_REMOTE_TEST=true` (hits real Redshift, Snowflake, BigQuery). Uploads coverage to Codecov.
- `lint` — Runs `sbt scalafmtSbtCheck scalafmtCheck test:scalafmtCheck`.
- `docker` (needs `test`) — Builds the Docker image from `./distrib/docker` using the version parsed from `version.sbt`, loads it locally, and runs `docker run … help` as a smoke test. Does **not** push.

### `test-only.yml` — test-only
**Trigger:** manual; `pull_request_target` (opened/synchronize/reopened) targeting `master` or `branch-*`.
**Purpose:** Runs the full SBT test suite on PRs without publishing artifacts. Posts a Slack event at the start.

### `test-bq-transform.yml` — Test BQ Transform
**Trigger:** `workflow_dispatch` only.
**Purpose:** Narrow run for BigQuery transform tests. Authenticates to GCP and runs the relevant SBT test target.

### `snapshot-jar-only.yml`
**Trigger:** `workflow_dispatch`.
**Purpose:** Builds the core assembly at the current `-SNAPSHOT` version and publishes it (via `GITHUB_TOKEN`) as an asset on the `v<version>-SNAPSHOT` GitHub pre-release, posting a Slack event via the `SLACK_STARLAKE_CORE_WEBHOOK_URL` webhook. Does not build a Docker image. Triggers the `starlake-api` `snapshot-jar-only.yml` workflow, which uploads the api zip to the same pre-release. That upload is the only publication channel for starlake-api (and the UI bundled inside it): both ship solely as part of the starlake-core release.

### `snapshot-docker-no-tests.yml`
**Trigger:** `workflow_dispatch`; or `workflow_run` after `snapshot-jar-only` completes successfully.
**Jobs:**
- `snapshot` — Deletes prior `-SNAPSHOT` packages, then publishes a fresh snapshot (`sbt ++2.13 update clean compile publish`). **No tests.**
- `docker-build` (matrix: `amd64`, `arm64`) — Builds and pushes per-platform images by digest to Docker Hub.
- `merge` — Combines per-platform digests into a single multi-arch manifest tagged with `SL_VERSION`.

### `snapshot-docker-with-tests.yml` — snapshot-docker
**Trigger:** `workflow_dispatch` only.
**Purpose:** Same shape as `snapshot-docker-no-tests` but runs the full test suite before publishing. Use when you want test validation alongside the snapshot Docker image.

### `release.yml` — Release
**Trigger:** `workflow_dispatch`; `push` of any tag matching `v*.*.*` except `v*-SNAPSHOT` (the negated pattern keeps snapshot pre-release tags from firing a production release).
**Jobs:**
- `docker-hub` — Reads the version from `.versions`/`version.sbt`, logs into Docker Hub, builds, and pushes images tagged with `SL_VERSION`, `MAJOR.MINOR`, `MAJOR`, and `latest`. This is the GitHub-release Docker build; it does not publish to Sonatype. Like every core workflow that publishes a Docker image (`release-docker.yml`, `snapshot-docker-*.yml`), it ends by dispatching `docker-hub-amd-arm.yml` in the `starlake-api` repo so the api image (which cohosts the UI) is rebuilt alongside the core image. That api workflow is dispatch-only and there is no separate starlake-ui image.

### `release-docker.yml`
**Trigger:** `workflow_dispatch`.
**Purpose:** Rebuilds the Docker image from the **latest released JAR on Maven Central** (no SBT compile/test). Useful for hotfix-tagging or publishing a Docker image when only the Dockerfile changed.
**Jobs:**
- `docker-build` (matrix: `amd64`, `arm64`) — Reads `release` from `maven-metadata.xml`, overrides `version.sbt`, builds and pushes per-platform images by digest.
- `merge` — Creates the multi-arch manifest tagged `SL_VERSION` and `latest`.

### `scala-steward.yml`
**Trigger:** `workflow_dispatch`; cron `0 0 * * 0` (every Sunday 00:00 UTC).
**Purpose:** Runs `scala-steward-org/scala-steward-action` to open PRs that bump SBT dependencies and plugins.
**Permissions:** `contents: write`, `pull-requests: write`.

### `slack.yml` — Slack Notification
**Trigger:** `push`, `pull_request` (opened/synchronize/reopened/ready_for_review), `issues` (most types), `issue_comment` (created/edited/deleted), `create`, `delete`.
**Purpose:** Posts the event to the channel behind `SLACK_STARLAKE_CORE_WEBHOOK_URL` via `8398a7/action-slack`.

## Required secrets

Workflows read these from repository secrets:

| Secret | Used by |
|---|---|
| `GCP_SERVICE_ACCOUNT`, `GCP_PROJECT`, `TEMPORARY_GCS_BUCKET` | build, release, snapshot-docker-*, test-only, test-bq-transform |
| `REDSHIFT_*` (DATABASE/HOST/USER/PASSWORD/ROLE) | build, snapshot-docker-*, test-only |
| `SNOWFLAKE_*` (ACCOUNT/DB/USER/PASSWORD/WAREHOUSE) | build, snapshot-docker-*, test-only |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ACCOUNT_ID` | build, snapshot-docker-*, test-only |
| `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN` | release, snapshot-docker-*, release-docker |
| `STARLAKE_API_TOKEN` | release (cross-repo dispatch) |
| `SLACK_STARLAKE_CORE_WEBHOOK_URL` | slack, snapshot-*, test-only |

## Common environment

- **JDK:** 17 (Zulu or Temurin via `actions/setup-java@v4`)
- **SBT:** installed via `sbt/setup-sbt@v1`
- **Scala:** 2.13.x (publish step pins `++2.13` or `++2.13.14`)
- **Heap:** `SBT_OPTS="-Xss4M -Xms1g -Xmx4g"`
- **Spark:** 4.1.3 (provided; PySpark installed only in `build.yml` for tests)
- **Version source:** parsed from `version.sbt` (or Maven Central metadata in `release-docker.yml`)
- **Docker context:** `./distrib/docker` (prepared by `./scripts/docker-prepare.sh`)
- **Image:** `starlakeai/starlake` on Docker Hub