# Spark 4 Migration Implementation Plan (starlake-core, starlake-streaming, starlake-api)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate starlake-core, starlake-streaming and starlake-api from Spark 3.5.8 to Spark 4.1.3 as a hard cutover (no dual Spark 3/4 support), with aligned dependency versions across all three repos.

**Architecture:** starlake-core migrates first (build pins, source fixes for removed/moved Spark internals, packaging/installer updates). starlake-streaming's existing `spark4` branch (already on Spark 4.0.1) is realigned to 4.1.3 and released so core can depend on it. starlake-api is last: it has zero direct Spark code, so its migration is build pins, `.versions`, `lib/` jars, Docker and CI. All work happens on a `spark4` branch in each repo.

**Tech Stack:** Scala 2.13.18, SBT 1.11.5, JDK 17 (build) / 21 (docker), Spark 4.1.3, Hadoop 3.4.2, Jackson 2.21.2.

## Global Constraints

The target version matrix. Every task uses these exact values; do not substitute.

| Component | Spark 3 (today) | Spark 4 target | Notes |
|---|---|---|---|
| Spark | 3.5.8 | **4.1.3** | 4.2.0 rejected: no Delta/Iceberg/BQ/Snowflake builds |
| Scala | 2.13.18 (core) / 2.13.16 (api) | **2.13.18 everywhere** | Spark 4.1.3 ships 2.13.17; 2.13.18 is binary compatible |
| Jackson | 2.15.2 | **2.21.2** | Exactly what Spark 4.1.3 ships (via jackson-bom) |
| Hadoop | 3.3.6 (+3.4.2 azure drift) | **3.4.2** | common/aws/azure must match exactly |
| AWS SDK | com.amazonaws:aws-java-sdk-bundle:1.12.794 | **software.amazon.awssdk:bundle:2.29.52** | Hadoop 3.4.x moved to SDK v2 |
| Delta | io.delta:delta-spark:3.3.2 | **io.delta:delta-spark_4.1 (%% → \_2.13):4.3.1** | Per-Spark-minor artifact ids now |
| spark-xml | com.databricks:spark-xml:0.18.0 | **REMOVED — built-in `xml` source** | XSDToSchema moves to Spark internal class |
| BigQuery spark connector | spark-bigquery-with-dependencies_2.13:0.44.0 | **com.google.cloud.spark:spark-4.1-bigquery:0.44.2-preview** | Plain `%`, NO scala suffix. Preview is the only 4.1 build |
| Snowflake spark connector | 3.1.8 | **3.2.1-spark_4.1** | Requires snowflake-jdbc >= 4.0.2 |
| snowflake-jdbc | 3.28.0 | **4.3.3** | |
| gcs-connector | hadoop3-2.2.32 | **4.0.4** (shaded classifier) | Built against Hadoop 3.4.2; URL scheme changes |
| bigquery-connector (MR) | hadoop3-1.2.0 | **hadoop3-1.2.0 (unchanged)** | |
| elasticsearch-spark | 8.16.3 (already excluded) | **STAYS EXCLUDED** | No Spark 4 artifact released yet (merged upstream, next minor) |
| Iceberg runtime (installer) | iceberg-spark-runtime-3.5_2.13:1.10.0 | **iceberg-spark-runtime-4.1_2.13:1.11.0** | |
| scala-parallel-collections | 1.0.4 | **1.2.0** | Matches Spark 4.1.3 |
| scala-collection-compat | 2.12.0 | **REMOVED** | Imported nowhere in src/ |
| json-schema-validator | 1.4.0 | **1.4.0 (unchanged)** | 2.0.4 bump is a follow-up (API changes); 3.x forbidden (Jackson 3) |
| guava override | 31.1-jre | **33.4.8-jre** | Matches Spark 4.1.3 distro |
| protobuf override | 3.25.8 | **4.33.0** | Matches Spark 4.1.3; verify BigQuery tests (Task 10) |
| pyspark (CI) | 3.5.8 | **4.1.3** | |
| redshiftJDBC | 2.1.0.34 | **2.2.8** | |
| ai.starlake spark-redshift (test) | 6.5.1 | **temporarily removed** | Fork rebuild is a follow-up workstream |
| starlake-streaming | 1.3.5 | **1.4.0** (released from its spark4 branch) | |
| starlake-core version | 1.7.2-SNAPSHOT | **1.8.0-SNAPSHOT** | Breaking change, clean break |
| sbt | 1.11.5 (core) / 1.11.2 (api) | **1.11.5 everywhere** | |
| JDK | 17 build / 21 docker | **unchanged** | Spark 4.1 supports both |

Repos and branches:
- `/Users/hayssams/git/public/starlake` — branch `spark4` (created)
- `/Users/hayssams/git/public/starlake-streaming` — branch `spark4` (pre-exists, HEAD = "Upgrade to Spark 4" on 4.0.1)
- `/Users/hayssams/git/starlake-api` — branch `spark4` (created)

Behavioral policy: **preserve Spark 3 semantics at first**. Spark 4 flips `spark.sql.ansi.enabled` to true globally; we pin it back to false in `reference-spark.conf` (Task 8) and embrace ANSI in a separate follow-up. Same for JDBC legacy type-mapping flags: only set them if tests fail (Task 9).

Verification baseline: `sbt compile` and `sbt test` must be run with JDK 17. Commit at the end of every task with the message given in the task.

---

## Phase A — starlake-core build definition

### Task 1: Core version pins (Versions.scala, Dependencies.scala, build.sbt)

**Files:**
- Modify: `project/Versions.scala`
- Modify: `project/Dependencies.scala`
- Modify: `build.sbt`
- Modify: `version.sbt`

**Interfaces:**
- Produces: `Versions.spark4`, `Versions.jacksonForSpark4`, `Dependencies.spark4`, `Dependencies.jacksonForSpark4` — names used by build.sbt and referenced by later tasks.

- [ ] **Step 1: Rewrite `project/Versions.scala`**

Replace the changed entries (keep all others as-is):

```scala
object Versions {
  val spark4 = "4.1.3"
  val deltaSpark = "4.3.1" // artifact id is delta-spark_4.1
  val scalatest = "3.2.19"
  val scalacheckForScalatest = "3.2.19.0"
  // sparkXML / sparkXML2d0 DELETED: Spark 4 has a built-in xml data source
  val springBoot = "2.0.6.RELEASE"
  val typesafeConfig = "1.4.6"
  val scalaLogging = "3.9.6"
  val hive = "3.1.0"
  val log4s = "1.3.3"
  val swaggerParser = "2.1.41"
  val betterFiles = "3.9.2"
  val jacksonForSpark4 = "2.21.2" // exactly what Spark 4.1.3 ships via jackson-bom
  val pureConfig = "0.17.9"
  // elasticsearch-spark has no Spark 4 build yet (merged upstream 2026-07, not released).
  // Re-enable esSpark212 in build.sbt when elasticsearch-spark-41_2.13 ships.
  val esSpark = "8.16.3"
  // json-schema-validator 2.0.4 is the last Jackson-2 line (3.x uses Jackson 3 = tools.jackson).
  // Bumping 1.4.0 -> 2.0.4 changes JsonSchemaFactory APIs: follow-up task, not part of this migration.
  val jsonSchemaValidator = "1.4.0"
  val scopt = "4.1.0"
  val bigquery = "2.49.0"
  val gcsConnector = "4.0.4" // new versioning scheme, built against Hadoop 3.4.2
  val hadoop = "3.4.2" // must match Spark 4.1.3's Hadoop line; aws/azure artifacts use this too
  val awsSdkBundle = "2.29.52" // software.amazon.awssdk (v2), pinned by hadoop-project 3.4.2
  val sparkBigquery = "0.44.2-preview" // artifact spark-4.1-bigquery, no scala suffix, GA build not yet published for 4.1
  val bigqueryConnector = "hadoop3-1.2.0"
  val h2 = "2.3.232" // Test only
  val poi = "4.1.2"
  val confluentVersion = "7.7.5"
  val kafkaClients = "7.7.5-ce"
  val testContainers = "0.44.0"
  val gcpCloudLogging = "3.23.10"
  val gcpDataCatalog = "1.79.0"
  val jinja = "2.7.4" // forces dependency override on guava
  val snowflakeJDBC = "4.3.3" // spark-snowflake 3.2.x requires >= 4.0.2
  val snowflakeSpark: String = "3.2.1-spark_4.1"
  val duckdb = "1.5.5.1"
  val bigQueue = "0.7.0"
  val redshiftJDBC = "2.2.8"
  val scalaParallelCollections = "1.2.0" // matches Spark 4.1.3
  val derbyVersion =
    "10.15.2.0" // last version compatible with Java 11, see https://db.apache.org/derby/derby_downloads.html
  val jSqlParser = "5.3.242"
  val jSqlTranspiler = "1.10"
  val starlakejdbc = "0.7"
  val airflowTemplates = "0.6.14"
  val dagsterTemplates = "0.5.9"
  val orchestrationTemplates = "0.5.6.1"
  val snowflakeTemplates = "0.4.1"
  val starlakeStreaming = "1.4.0" // Spark 4 build, released from starlake-streaming spark4 branch
}
```

Deleted entries: `spark3`, `deltaSpark3d0`, `sparkXML`, `sparkXML2d0`, `jacksonForSpark3`, `scalaCompat`. Renamed: `sparkBigqueryWithDependencies` → `sparkBigquery`. Added: `awsSdkBundle`, `starlakeStreaming`.

- [ ] **Step 2: Update `project/Dependencies.scala`**

Rename `jacksonForSpark3` → `jacksonForSpark4` (Seq name and `Versions.` reference, 6 lines at 53-60).

Replace the `spark3` Seq (lines 62-86) with:

```scala
  val spark4 = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark4 % "provided" exclude (
      "com.google.guava",
      "guava"
    ) excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-sql" % Versions.spark4 % "provided" exclude (
      "com.google.guava",
      "guava"
    ) excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-hive" % Versions.spark4 % "provided" exclude (
      "com.google.guava",
      "guava"
    ) excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-mllib" % Versions.spark4 % "provided" exclude (
      "com.google.guava",
      "guava"
    ) excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-sql-kafka-0-10" % Versions.spark4 excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-avro" % Versions.spark4 excludeAll (jacksonExclusions: _*),
    "io.delta" %% "delta-spark_4.1" % Versions.deltaSpark % "provided" exclude (
      "com.google.guava",
      "guava"
    ) excludeAll (jacksonExclusions: _*)
  )
```

(The `com.databricks %% spark-xml` entry is gone; `delta-spark` became `delta-spark_4.1`.)

Update `hadoop` Seq (lines 88-109): no structural change, `Versions.hadoop` now resolves to 3.4.2.

Update `azure` Seq: replace hardcoded `"3.4.2"` with `Versions.hadoop` (they now agree; make it a single source of truth):

```scala
  val azure = Seq(
    "org.apache.hadoop" % "hadoop-azure" % Versions.hadoop % "provided" excludeAll (jacksonExclusions: _*) exclude (
      "com.google.guava",
      "guava"
    ),
    "com.microsoft.azure" % "azure-storage" % "8.6.6" % "provided" excludeAll (jacksonExclusions: _*) exclude (
      "com.google.guava",
      "guava"
    )
  )
```

Replace the `redshift` Seq (lines 127-131) — hadoop-aws must equal `Versions.hadoop`, and the v1 AWS bundle becomes the v2 bundle:

```scala
  val redshift = Seq(
    "com.amazon.redshift" % "redshift-jdbc42" % Versions.redshiftJDBC % "provided" excludeAll (jacksonExclusions: _*),
    "org.apache.hadoop" % "hadoop-aws" % Versions.hadoop % "provided" excludeAll (jacksonExclusions: _*),
    "software.amazon.awssdk" % "bundle" % Versions.awsSdkBundle % "provided" excludeAll (jacksonExclusions: _*)
  )
```

In the `gcp` Seq: `gcsConnectorShadedJar` (line 162-163) already interpolates `Versions.gcsConnector`, so the URL becomes `.../gcs-connector/4.0.4/gcs-connector-4.0.4-shaded.jar` — verify with the curl in Step 4. Replace the spark-bigquery line (174) with the suffix-less Spark 4 artifact:

```scala
    "com.google.cloud.spark" % "spark-4.1-bigquery" % Versions.sparkBigquery % "provided" excludeAll (jacksonExclusions: _*),
```

Replace `scalaCompat` (lines 260-263) — collection-compat is imported nowhere in src/, parallel-collections is still used by `TransformWorkflow.scala:151`:

```scala
  val scalaCompat = Seq(
    "org.scala-lang.modules" %% "scala-parallel-collections" % Versions.scalaParallelCollections
  )
```

In `scala213LibsOnly` (line 256-258): remove the `ai.starlake %% spark-redshift % "6.5.1" % Test` entry (Spark 3 build; fork rebuild is a follow-up — see Task 19). Leave the Seq empty:

```scala
  val scala213LibsOnly = Seq.empty[ModuleID]
```

Update `starlakeStreaming` (line 275-277):

```scala
  val starlakeStreaming = Seq(
    "ai.starlake" %% "starlake-streaming" % Versions.starlakeStreaming % "provided"
  )
```

NOTE: until Task 14 (streaming released), this resolves only from `~/.ivy2/local` via `sbt publishLocal` in the streaming repo. That is acceptable on the branch.

- [ ] **Step 3: Update `build.sbt`**

Line 63-74, the libraryDependencies block: rename `spark3` → `spark4`, `jacksonForSpark3` → `jacksonForSpark4`, and fix the es comment:

```scala
libraryDependencies ++= {
  val versionSpecificLibs = {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 13)) => scalaCompat ++ scala213LibsOnly
      case _ => throw new Exception(s"Invalid Scala Version")
    }
  }
  dependencies(isSnapshot.value) ++ spark4 ++
    jacksonForSpark4 ++ // esSpark212 excluded: no elasticsearch-spark release supports Spark 4 yet
    pureConfig ++ scalaReflection(scalaVersion.value) ++
    versionSpecificLibs
}
```

Line 76-84, dependencyOverrides — align with the Spark 4.1.3 distro:

```scala
dependencyOverrides := Seq(
  "com.google.protobuf"                % "protobuf-java"             % "4.33.0",
  "org.scala-lang"                    % "scala-library"             % scalaVersion.value,
  "org.scala-lang"                    % "scala-reflect"             % scalaVersion.value,
  "org.scala-lang"                    % "scala-compiler"            % scalaVersion.value,
  "com.google.guava"                  %  "guava"                    % "33.4.8-jre", // match Spark 4.1.3; jinjava is fine with it
  "com.fasterxml.jackson.dataformat"  % "jackson-dataformat-csv"    % Versions.jacksonForSpark4,
  "com.manticore-projects.jsqlformatter" % "jsqlparser"             % Versions.jSqlParser // avoid MethodTooLargeException during assembly shading
)
```

- [ ] **Step 4: Verify dependency resolution (not compilation yet)**

```bash
cd /Users/hayssams/git/public/starlake
curl -sSf -o /dev/null -w "%{http_code}\n" https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/4.0.4/gcs-connector-4.0.4-shaded.jar -r 0-0
sbt update
```
Expected: `206` (or `200`) from curl; `sbt update` succeeds with no unresolved dependencies. Compilation is NOT expected to pass yet (source fixes come next).

- [ ] **Step 5: Bump `version.sbt`**

```scala
ThisBuild / version := "1.8.0-SNAPSHOT"
```

- [ ] **Step 6: Commit**

```bash
git add project/Versions.scala project/Dependencies.scala build.sbt version.sbt
git commit -m "build: move dependency matrix to Spark 4.1.3 / Hadoop 3.4.2 / Jackson 2.21.2"
```

---

## Phase B — starlake-core source fixes

Run `sbt compile` after each task; the error count must strictly decrease. Order is chosen so each task removes a distinct error cluster.

### Task 2: Delete dead Spark-internal files, fix `FileStreamSource.Timestamp` imports

**Files:**
- Delete: `src/main/scala/org/apache/spark/sql/classic/ai/starlake/http/HttpSourceProxy.scala` (unreferenced in core; the live copy is in starlake-streaming)
- Delete: `src/main/scala/org/apache/spark/sql/catalyst/parser/ParserSQL.scala` (entirely commented out)
- Delete: `src/main/scala/BigQueryLoad.scala` (scratch main class in default package)
- Modify: `src/main/scala/ai/starlake/schema/handlers/HdfsStorageHandler.scala:29`
- Modify: `src/main/scala/ai/starlake/schema/handlers/StorageHandler.scala:28`
- Modify: `src/main/scala/ai/starlake/schema/handlers/LocalStorageHandler.scala:28`
- Modify: `src/main/scala/ai/starlake/job/metrics/MetricsJob.scala:11`

- [ ] **Step 1: Verify HttpSourceProxy is truly unreferenced, then delete the three files**

```bash
grep -rn "HttpSourceProxy" src/ --include="*.scala" | grep -v "org/apache/spark/sql/classic"
git rm src/main/scala/org/apache/spark/sql/classic/ai/starlake/http/HttpSourceProxy.scala
git rm src/main/scala/org/apache/spark/sql/catalyst/parser/ParserSQL.scala
git rm src/main/scala/BigQueryLoad.scala
```
Expected: the grep prints nothing (if it prints call sites, do NOT delete; instead port the starlake-streaming `classic` version with `ClassicConversions.castToImpl`).

- [ ] **Step 2: Replace the Timestamp alias in the 4 files**

`org.apache.spark.sql.execution.streaming.FileStreamSource` moved to `...streaming.runtime` in Spark 4.1, and `FileStreamSource.Timestamp` is just `type Timestamp = Long`. In each of the 4 files: delete the import line `import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp` and replace every use of the type `Timestamp` in those files with `Long`.

Caution: check each file for `java.sql.Timestamp` imports before doing a blind rename; only the alias usages change.

- [ ] **Step 3: Compile and commit**

```bash
sbt compile 2>&1 | tail -20
git add -A src/main/scala
git commit -m "refactor: drop Spark-internal FileStreamSource.Timestamp alias and dead Spark-package files"
```
Expected: the FileStreamSource and deleted-file errors are gone; remaining errors belong to Tasks 3-6.

### Task 3: XML — built-in data source and XSDToSchema

**Files:**
- Modify: `src/main/scala/ai/starlake/schema/handlers/SchemaHandler.scala:55,1772`
- Modify: `src/main/scala/ai/starlake/job/ingest/XmlIngestionJob.scala:72,100`
- Modify: `src/main/scala/ai/starlake/job/infer/InferSchemaJob.scala:223,233`
- Modify: `src/test/scala/ai/starlake/schema/handlers/InferSchemaInfoHandlerSpec.scala:96,130,149`

- [ ] **Step 1: Switch format strings**

Replace `.format("com.databricks.spark.xml")` with `.format("xml")` at `XmlIngestionJob.scala:72` and `:100` and `InferSchemaJob.scala:223`, plus `InferSchemaInfoHandlerSpec.scala:149`. Options (`rowTag`, `rootTag`, `inferSchema`, `encoding`) carry over unchanged — the built-in source was ported from spark-xml with the same option names.

Also replace `.format("com.databricks.spark.csv")` with `.format("csv")` and drop the dead `.option("parserLib", "UNIVOCITY")` at `InferSchemaJob.scala:233` and `InferSchemaInfoHandlerSpec.scala:96,130` (the alias only survives via Spark's backwardCompatibilityMap; univocity is the only CSV parser since Spark 3).

- [ ] **Step 2: Replace XSDToSchema import**

In `SchemaHandler.scala:55`, replace:

```scala
import com.databricks.spark.xml.util.XSDToSchema
```
with:
```scala
import org.apache.spark.sql.execution.datasources.xml.XSDToSchema
```

At line 1772 the call is `XSDToSchema.read(xsdContent)` where `xsdContent` is a String. Spark 4's `XSDToSchema` was ported from spark-xml; if `read(String)` is absent in 4.1.3 (check the compile error), the available overload is `read(xsdPath: Path)` — in that case write the content to a temp file, or copy the ~40-line `read(String)` body from spark-xml 0.18.0 into a small `ai.starlake.utils.XsdToSparkSchema` helper. Prefer the direct import if it compiles; it is Spark-internal but core already lives with several such couplings.

- [ ] **Step 3: Compile and commit**

```bash
sbt compile 2>&1 | tail -20
git add -A src
git commit -m "feat(xml): use Spark 4 built-in xml data source, drop com.databricks:spark-xml"
```

### Task 4: `DatasetLogging` via the classic API

**Files:**
- Modify: `src/main/scala/org/apache/spark/sql/DatasetLogging.scala`

In Spark 4, `org.apache.spark.sql.Dataset` is the abstract Connect-shared type; `showString` lives on `org.apache.spark.sql.classic.Dataset`. Use the official cast:

- [ ] **Step 1: Rewrite the trait**

```scala
package org.apache.spark.sql

import org.apache.spark.sql.classic.ClassicConversions.castToImpl

trait DatasetLogging {
  implicit class DatasetHelper[T](ds: Dataset[T]) {
    def showString(numRows: Int = 20, truncate: Int = 20, vertical: Boolean = false): String =
      castToImpl(ds).showString(numRows, truncate, vertical)

    def schemaString(): String = ds.schema.treeString
  }
}
```

- [ ] **Step 2: Compile and commit**

```bash
sbt compile 2>&1 | tail -20
git add src/main/scala/org/apache/spark/sql/DatasetLogging.scala
git commit -m "fix(spark4): route DatasetLogging.showString through classic Dataset"
```

### Task 5: JDBC dialects and JdbcUtils signature changes

**Files:**
- Modify: `src/main/scala/ai/starlake/sql/StarlakeJdbcDialects.scala` (classifyException at :214, plus any other compile errors in the file)
- Modify: `src/main/scala/ai/starlake/utils/SparkUtils.scala` (JdbcUtils.getSchema/getJdbcType call sites at :130,139,375,392)

- [ ] **Step 1: Port `classifyException` in the MySQL dialect**

Spark 4 removed `classifyException(message: String, e: Throwable)`. The replacement (check the exact signature in the 4.1.3 `JdbcDialect` source — `sql/core/src/main/scala/org/apache/spark/sql/jdbc/JdbcDialect.scala` at tag v4.1.3) is:

```scala
  override def classifyException(
    e: Throwable,
    condition: String,
    messageParameters: Map[String, String],
    description: String,
    isRuntime: Boolean
  ): Throwable with SparkThrowable = {
    e match {
      case sqlException: SQLException =>
        sqlException.getErrorCode match {
          // ER_DUP_KEYNAME
          case 1061 =>
            // description is: Failed to create index indexName in tableName
            val regex = "(?s)Failed to create index (.*) in (.*)".r
            regex.findFirstMatchIn(description) match {
              case Some(m) =>
                throw new IndexAlreadyExistsException(
                  indexName = m.group(1),
                  tableName = m.group(2),
                  cause = Some(e)
                )
              case None =>
                super.classifyException(e, condition, messageParameters, description, isRuntime)
            }
          case 1091 =>
            val regex = "(?s)Failed to drop index (.*) in (.*)".r
            regex.findFirstMatchIn(description) match {
              case Some(m) =>
                throw new NoSuchIndexException(m.group(1), m.group(2), cause = Some(e))
              case None =>
                super.classifyException(e, condition, messageParameters, description, isRuntime)
            }
          case _ => super.classifyException(e, condition, messageParameters, description, isRuntime)
        }
      case unsupported: UnsupportedOperationException => throw unsupported
      case _ => super.classifyException(e, condition, messageParameters, description, isRuntime)
    }
  }
```

Add `import org.apache.spark.SparkThrowable` to the file's imports.

- [ ] **Step 2: Fix remaining compile errors in `StarlakeJdbcDialects.scala` and `SparkUtils.scala`**

Drive by compiler output. Known changes in Spark 4: `JdbcUtils.getSchema` gained parameters (an added `conn`/dialect-first reordering — mirror the new signature from the 4.1.3 source and pass the existing values through), `JdbcSQLQueryBuilder` and `createConnectionFactory` signatures may have shifted. Do NOT redesign anything here; make the minimal change each compile error dictates.

- [ ] **Step 3: Compile and commit**

```bash
sbt compile 2>&1 | tail -20
git add src/main/scala/ai/starlake/sql/StarlakeJdbcDialects.scala src/main/scala/ai/starlake/utils/SparkUtils.scala
git commit -m "fix(spark4): adapt JDBC dialects and JdbcUtils call sites to Spark 4 signatures"
```

### Task 6: Remaining classic/internal API call sites — full compile sweep

**Files (drive the exact list from compiler output):**
- Modify: `src/main/scala/org/apache/spark/sql/execution/datasources/duckdb/DuckDBRelationProvider.scala` (`sqlContext.sparkSession.sessionState` → `castToImpl(sqlContext.sparkSession).sessionState`, import `ClassicConversions.castToImpl`)
- Modify: `src/main/scala/ai/starlake/job/transform/SparkAutoTask.scala:131` (`session.sessionState.sqlParser` → `castToImpl(session).sessionState.sqlParser`)
- Modify: `src/main/scala/org/apache/spark/sql/execution/datasources/json/JsonIngestionUtil.scala` (catalyst JSON internals; adjust imports/usages per compile errors — the file is a vendored copy of Spark's InferSchema, so re-vendoring the 4.1.3 version of the corresponding logic is acceptable if patching is messier)
- Modify: `src/main/scala/ai/starlake/utils/SparkUtils.scala:438` (`org.apache.spark.deploy.PythonRunner.main` — verify it still resolves; it exists in Spark 4 but confirm)
- Modify: `src/test/scala/ai/starlake/TestHelper.scala:440` and `src/test/scala/ai/starlake/job/load/PositionIngestionJobSpec.scala:114,143` (`sparkSession.sessionState.catalog` → classic cast, same pattern)
- Modify: `src/main/scala/ai/starlake/job/sink/kafka/KafkaJob.scala:279-286` (recheck the RowEncoder workaround comment still applies; the foreachBatch path itself is public API and should compile unchanged)

- [ ] **Step 1: Iterate `sbt compile` to zero errors**

```bash
sbt compile 2>&1 | grep -E "^\[error\]" | head -40
```
Apply the classic-cast pattern (`import org.apache.spark.sql.classic.ClassicConversions.castToImpl`) wherever `sessionState`/classic-only members are the error. Keep edits minimal and mechanical.

- [ ] **Step 2: Compile test sources to zero errors**

```bash
sbt Test/compile 2>&1 | grep -E "^\[error\]" | head -40
```
Expected additional break: `RedshiftSpec` (or similar) referencing the removed `ai.starlake spark-redshift` test dependency — mark those specs `ignore` with a `// TODO spark4: re-enable when ai.starlake:spark-redshift is rebuilt for Spark 4` comment rather than deleting them.

- [ ] **Step 3: Commit**

```bash
git add -A src
git commit -m "fix(spark4): adapt remaining classic/catalyst call sites; core compiles on Spark 4.1.3"
```

---

## Phase C — starlake-core behavior and tests

### Task 7: Behavioral parity configuration

**Files:**
- Modify: `src/main/resources/reference-spark.conf`
- Modify: `src/main/resources-other/fs/reference.conf:402`
- Modify: `src/main/resources-other/gcp/reference.conf:195`

- [ ] **Step 1: Pin ANSI off for parity**

In `reference-spark.conf`, add (alongside the existing `sql.*` keys):

```hocon
  # Spark 4 enables ANSI mode by default. Starlake 1.8 keeps Spark 3 semantics for now:
  # stricter-cast/overflow/div-by-zero errors would change ingestion typing behavior.
  # Embracing ANSI is tracked as a follow-up. SparkEnv still forces ANSI on for Iceberg.
  sql.ansi.enabled = "false"
```

Note: `SparkEnv.scala:165` forces `spark.sql.ansi.enabled=true` on the Iceberg branch AFTER loading this config — verify that ordering still holds (config.set wins over the conf-file default; it does today, keep it).

- [ ] **Step 2: Fix the stale legacy rebase keys**

`resources-other/fs/reference.conf:402` and `resources-other/gcp/reference.conf:195` use `sql.legacy.parquet.datetimeRebaseModeInWrite` — the `legacy.` prefix was already wrong for Spark 3.5 and the keys are silently ignored. Rename both to `sql.parquet.datetimeRebaseModeInWrite` to match `reference-spark.conf:24-31`.

- [ ] **Step 3: Commit**

```bash
git add src/main/resources src/main/resources-other
git commit -m "config(spark4): keep ANSI off for Spark 3 parity; fix stale datetime rebase keys"
```

### Task 8: Test suite green

- [ ] **Step 1: Run the full suite**

```bash
sbt test 2>&1 | tee /tmp/spark4-test-run.log | grep -E "TESTS FAILED|All tests passed|\*\*\* FAILED"
```
Tests run sequentially and forked; expect a long run (~4GB heap per fork).

- [ ] **Step 2: Triage failures by cluster**

Expected clusters and their fixes:
1. **ANSI leakage** (cast/overflow errors despite Task 7): a code path building its own SparkConf without reference-spark.conf — fix the config plumbing, not the test.
2. **JDBC type-mapping drift** (SMALLINT→Short, FLOAT→Float, Postgres/MySQL timestamp mapping): first try the matching `spark.sql.legacy.*Mapping.enabled` flag in `reference-spark.conf` for parity; if the new mapping is genuinely better, change the expected values in the test and note it in the commit message.
3. **ES spec**: `ESLoadJobSpec` is not `ignore`d today — if it fails at runtime resolution of `org.elasticsearch.spark.sql`, mark it `ignore` with `// TODO spark4: re-enable when elasticsearch-spark ships Spark 4 support`.
4. **CREATE TABLE default provider** (Hive vs `spark.sql.sources.default`): prefer setting `spark.sql.legacy.createHiveTableByDefault=false` semantics consciously — Starlake creates tables via explicit `USING`/format almost everywhere, so failures here are likely test-fixture-only.
5. **Derby/Hive metastore**: derby 10.15.2.0 against Spark 4's Hive 2.3.10 — same Hive line as Spark 3.5, should not regress; investigate before touching versions.

- [ ] **Step 3: Commit fixes**

```bash
git add -A src
git commit -m "test(spark4): full suite green on Spark 4.1.3"
```

---

## Phase D — starlake-core packaging, installer, CI

### Task 9: Assembly audit

**Files:**
- Modify: `build.sbt:148-206` (assemblyExcludedJars, assemblyShadeRules)

- [ ] **Step 1: Get the Spark 4.1.3 distro jar list**

```bash
cd /private/tmp/claude-501/-Users-hayssams-git-public-starlake/*/scratchpad
curl -LO https://archive.apache.org/dist/spark/spark-4.1.3/spark-4.1.3-bin-hadoop3.tgz
tar tzf spark-4.1.3-bin-hadoop3.tgz | grep '/jars/' | sed 's|.*/jars/||' | sort > spark4-distro-jars.txt
```
(Note: NO `-scala2.13` suffix in Spark 4 tarball names.)

- [ ] **Step 2: Re-audit `assemblyExcludedJars`**

Run `sbt assembly` and compare the printed `->` classpath listing against `spark4-distro-jars.txt`. Rules:
- A jar prefix stays in the exclusion list only if the distro provides that jar.
- Known changes to check explicitly: `lz4-java` (dropped from Spark 4 distro — REMOVE from exclusions if absent), Arrow jars (major version bump but same prefixes), Netty 4.2.x (same prefixes), `threeten-extra`, `flatbuffers-java`.
- Keep the shade rules (guava/gson/commons-compress) — Spark 4 still bundles guava (33.4.8) and the BigQuery client still needs its own.

- [ ] **Step 3: Build both assemblies and smoke the jar**

```bash
sbt assembly
sbt assemblyWithSpark
java -cp target/scala-2.13/starlake-core_2.13-1.8.0-SNAPSHOT-assembly.jar ai.starlake.job.Main --help 2>&1 | head -5
```
Expected: both succeed; Main prints usage. Commit:

```bash
git add build.sbt
git commit -m "build(spark4): re-audit assembly exclusions against the Spark 4.1.3 distribution"
```

### Task 10: Installer and runtime version pins (Setup.java, .versions, distrib)

**Files:**
- Modify: `src/main/java/Setup.java:338-419`
- Modify: `.versions`
- Modify: `distrib/starlake.sh`, `distrib/starlake.cmd` (only if they hardcode the scala-suffixed dir name — verify)

- [ ] **Step 1: Setup.java version defaults**

At lines 338-395 set: `SPARK_VERSION` default `4.1.3`, `SPARK_DELTA` `4.3.1`, `SPARK_ICEBERG` `1.11.0`, `SPARK_BQ_VERSION` `0.44.2-preview`, `HADOOP_AZURE_VERSION` `3.4.2`, `HADOOP_AWS_VERSION` `3.4.2`, `SPARK_SNOWFLAKE_VERSION` `3.2.1-spark_4.1`.

- [ ] **Step 2: Fix the two URL construction bugs-in-waiting**

Line 409 — Spark 4 tarballs have NO `-scala2.13` suffix. Change the URL construction from `spark-$V-bin-hadoop$H-scala2.13.tgz` to `spark-$V-bin-hadoop$H.tgz` (and the same for the extracted dir name if derived).

Line 419 — Iceberg artifact id: `iceberg-spark-runtime-3.5_2.13` → `iceberg-spark-runtime-4.1_2.13`.

Also line 363-365: winutils pinned to `hadoop-3.3.6` in the cdarlint repo — check `https://github.com/cdarlint/winutils` for a `hadoop-3.4.x` folder; if none exists, keep 3.3.6 (winutils is forward-compatible enough for local Windows use) and add a comment.

Also update the BigQuery jar download: the artifact changed from `spark-bigquery-with-dependencies_2.13` to `spark-4.1-bigquery` (no suffix) — fix wherever Setup.java builds that URL.

- [ ] **Step 3: Sync `.versions`**

```
SPARK_VERSION=4.1.3
HADOOP_VERSION=3
SPARK_BQ_VERSION=0.44.2-preview
SPARK_SNOWFLAKE_VERSION=3.2.1-spark_4.1
HADOOP_AWS_VERSION=3.4.2
HADOOP_AZURE_VERSION=3.4.2
```
(Leave `SPARK_REDSHIFT_VERSION` but add a `# TODO spark4` comment; leave DUCKDB as-is.) Verify `scripts/versions.sh` still parses the file.

- [ ] **Step 4: Test the installer end to end**

```bash
cd /private/tmp/claude-501/-Users-hayssams-git-public-starlake/*/scratchpad && mkdir -p sl-install-test && cd sl-install-test
cp /Users/hayssams/git/public/starlake/distrib/starlake.sh .
SL_VERSION=1.8.0-SNAPSHOT ./starlake.sh install 2>&1 | tail -20
```
Expected: downloads `spark-4.1.3-bin-hadoop3.tgz` and all connector jars without 404s. Commit:

```bash
git add src/main/java/Setup.java .versions distrib
git commit -m "install(spark4): Spark 4.1.3 tarball naming, connector pins, iceberg 4.1 runtime"
```

### Task 11: CI workflows and docs

**Files:**
- Modify: `.github/workflows/build.yml:32`, `.github/workflows/test-only.yml:52` — `pip install pyspark==4.1.3`
- Modify: `CLAUDE.md:94`, `docs/ARCHITECTURE.md:489`, `docs/GITHUB_ACTIONS.md:25,93` — mention Spark 4.1.3 instead of 3.5.8
- Verify: `scripts/pyspark-smoke-test.sh` — the py4j glob (`py4j-*-src.zip`) is version-agnostic; run it against the Task 10 install dir to confirm

- [ ] **Step 1: Apply edits, run a local docker smoke if feasible, commit**

```bash
git add .github docs CLAUDE.md
git commit -m "ci(spark4): pyspark 4.1.3, doc version references"
```

---

## Phase E — starlake-streaming

### Task 12: Realign the streaming `spark4` branch to 4.1.3

**Repo:** `/Users/hayssams/git/public/starlake-streaming`, branch `spark4` (already checked out).

**Files:**
- Modify: `project/Versions.scala` — `spark4 = "4.1.3"`, `jacksonForSpark4 = "2.21.2"` (currently 2.17.2, which is OLDER than what even Spark 4.0.1 ships), `deltaSpark4d0` → delete or bump to `4.3.1` with artifact `delta-spark_4.1` if delta is actually referenced in Dependencies.scala (verify; drop if unused)
- Modify: `project/Dependencies.scala:81` — `starlake-core` pin `1.5.3-SNAPSHOT` → `1.8.0-SNAPSHOT`
- Modify: `src/main/scala/ai/starlake/streaming/sink/http/HttpSource.scala:8` — split imports: `LongOffset` and `SerializedOffset` moved to `org.apache.spark.sql.execution.streaming.runtime` in Spark 4.1
- Modify: `src/test/scala/ai/starlake/job/sink/http/HttpProviderTest.scala:9,119` — `MemoryStream` moved to `...streaming.runtime` AND its constructor's second parameter changed from `sqlContext: SQLContext` to `sparkSession: SparkSession`
- Modify: `build.sbt` — strip the Sonatype release stack (`sonatypePublishToBundle`, `s01.oss.sonatype.org` refs) in line with the GitHub-Releases-only publication policy; remove `sbt-sonatype` from `project/plugins.sbt`
- Modify: `version.sbt` — `1.4.0-SNAPSHOT`

**Interfaces:**
- Consumes: starlake-core `1.8.0-SNAPSHOT` from `~/.ivy2/local` (publish it first: `cd /Users/hayssams/git/public/starlake && sbt publishLocal`)
- Produces: `ai.starlake:starlake-streaming_2.13:1.4.0-SNAPSHOT` via `sbt publishLocal`, consumed by core's Task 13.

- [ ] **Step 1: Publish core locally, apply the edits above**
- [ ] **Step 2: Compile and test**

```bash
cd /Users/hayssams/git/public/starlake-streaming
sbt compile test 2>&1 | tail -10
```
Expected: green. (The repo currently cannot even resolve — the core 1.5.3-SNAPSHOT transitive `jsqltranspiler:1.5-SNAPSHOT` is a 404 — so any successful resolution already proves the pin fix.)

- [ ] **Step 3: Publish locally and commit**

```bash
sbt publishLocal
git add -A
git commit -m "build: realign spark4 branch to Spark 4.1.3 / Jackson 2.21.2 / core 1.8.0-SNAPSHOT"
```

### Task 13: Wire streaming back into core

**Repo:** core. Verify `Versions.starlakeStreaming = "1.4.0"` resolves. While unreleased, temporarily use `1.4.0-SNAPSHOT` in `project/Versions.scala` and flip to `1.4.0` at release time (Task 18).

- [ ] **Step 1: `sbt update compile` in core against the locally published streaming jar; commit the pin.**

---

## Phase F — starlake-api

### Task 14: API build definition

**Repo:** `/Users/hayssams/git/starlake-api`, branch `spark4` (created).

**Files:**
- Modify: `build.sbt:13` — `scala213 = "2.13.18"`
- Modify: `build.sbt:165-172` — dependencyOverrides: protobuf `4.33.0`, guava `33.4.8-jre` (same values as core Task 1 Step 3)
- Modify: `project/build.properties` — `sbt.version=1.11.5`
- Modify: `project/Versions.scala` — `spark3` → `spark4 = "4.1.3"`, `jackson = "2.21.2"`, `snowflakeSpark = "3.2.1-spark_4.1"`, `snowflakeJDBC = "4.3.3"`, `redshiftJDBC = "2.2.8"`, delete `sparkBigqueryWithDependencies` in favor of `sparkBigquery = "0.44.2-preview"`
- Modify: `project/Dependencies.scala`:
  - `spark3` Seq (lines 32-37) → `spark4`, versions via `Versions.spark4`; drop the `cross CrossVersion.for3Use2_13` clauses (no-ops on 2.13, and Spark 4 is 2.13-only)
  - `azure` (line 109): hadoop-azure `3.4.1` → `3.4.2`
  - `redshift` (lines 124-128): hadoop-aws `3.3.6` → `3.4.2`; replace `com.amazonaws:aws-java-sdk-bundle:1.12.787` with `software.amazon.awssdk:bundle:2.29.52`
  - `bigquery` (lines 130-132): `"com.google.cloud.spark" % "spark-4.1-bigquery" % Versions.sparkBigquery % "provided" excludeAll (jacksonExclusions: _*)` (plain `%`, no scala suffix)
  - `snowflake` (line 121): drop the `cross` clause, version picks up 3.2.1-spark_4.1
- Modify: `.scala-steward.conf` — update the Jackson pin block to allow the 2.21.x line (it currently freezes Jackson entirely)
- Modify: `project/Common.scala:86` — TEMPORARILY remove `-Xfatal-warnings` (core's Spark 4 surface will emit new deprecation warnings); restore it as a follow-up once warnings are cleaned

- [ ] **Step 1: Apply edits; `sbt update` must resolve (compile still needs the new core jar — next task)**
- [ ] **Step 2: Commit**

```bash
git add build.sbt project .scala-steward.conf
git commit -m "build: Spark 4.1.3 / Scala 2.13.18 / Jackson 2.21.2 dependency matrix"
```

### Task 15: API runtime jars and `.versions` (including drift reconciliation)

**Files:**
- Modify: `.versions` — `SPARK_VERSION=4.1.3`, `SPARK_BQ_VERSION=0.44.2-preview`, `HADOOP_AZURE_VERSION=3.4.2`, `HADOOP_AWS_VERSION=3.4.2`, `SPARK_SNOWFLAKE_VERSION=3.2.1-spark_4.1`, `SNOWFLAKE_JDBC_VERSION=4.3.3`, and reconcile the pre-existing drifts: postgresql to `42.7.10` (Versions.scala value), `SPARK_REDSHIFT_VERSION` — keep with `# TODO spark4` comment
- Delete: `versions.sh` (repo root — orphaned decoy with SPARK_VERSION=3.5.3; nothing sources it)
- Delete: `scripts/spark-env.sh` (referenced by nothing)
- Modify: `scripts/dev_run.sh:28` — stale `spark-3.1.2-bin-hadoop3.2` path; fix to the 4.1.3 layout or delete the script if unused
- Refresh: `lib/` — rebuild core (`sbt assembly` in core, Spark 4 branch) and refresh via the existing script:

```bash
cd /Users/hayssams/git/public/starlake && sbt assembly
cd /Users/hayssams/git/starlake-api
SL_CORE_JAR=/Users/hayssams/git/public/starlake/target/scala-2.13/starlake-core_2.13-1.8.0-SNAPSHOT-assembly.jar ./scripts/download-lib.sh
```

- [ ] **Step 1: Apply, refresh lib/, then compile the API against the Spark 4 core**

```bash
sbt compile 2>&1 | tail -10
```
Expected: green — the API has no direct Spark code; failures here mean a core API the api calls changed (`SparkEnv.closeSession`, `IngestionJob.loadRequiresSpark`) — fix call sites minimally.

- [ ] **Step 2: Run api tests, commit**

```bash
sbt test 2>&1 | tail -10
git add -A
git commit -m "build: refresh lib/ against Spark 4 core; sync .versions; drop orphaned version files"
```

### Task 16: API Docker and CI

**Files:**
- Modify: `Dockerfile:6,63` — `SPARK_VERSION=4.1.3`
- Modify: `Dockerfile.local` — fix the internal inconsistency: single `ARG SPARK_VERSION` defaulted to `4.1.3` (lines 8/16 duplicate today), drop the hardcoded `ENV SPARK_VERSION="3.5.8"` at line 92, fix `SL_MAJOR_MINOR_VERSION` default `1.5` → `1.8`, `SCALA_VERSION` stays `2.13`
- Modify: `.github/workflows/docker-hub-amd-arm.yml` — add an `actions/setup-java` step (temurin 17) before the sbt-dependent `scripts/docker-prepare.sh` call, mirroring `snapshot-jar-only.yml:17-20`; verify the `--build-arg SPARK_VERSION` plumbing in `scripts/docker-build.sh:113-128` picks up `.versions`

Note: the vendored `starlake/starlake.sh` (gitignored) comes from core's `distrib/starlake.sh` at install time — it is fixed by core Task 10, nothing to do here beyond re-running the install.

- [ ] **Step 1: Apply, build the local docker image as a smoke test, commit**

```bash
./scripts/docker-build-local.sh 2>&1 | tail -5   # or the documented local build entry point
git add Dockerfile Dockerfile.local .github scripts
git commit -m "docker/ci: Spark 4.1.3 runtime, JDK 17 setup in docker-hub workflow"
```

---

## Phase G — integration, release ordering, follow-ups

### Task 17: Cross-repo end-to-end validation

- [ ] **Step 1: Fresh-install smoke using the Task 10 scratchpad install: run a bootstrap project load + transform on DuckDB (no cloud deps)**

```bash
cd /private/tmp/claude-501/-Users-hayssams-git-public-starlake/*/scratchpad/sl-install-test
SL_ROOT=$(mktemp -d) ./starlake.sh bootstrap 2>&1 | tail -3
```

- [ ] **Step 2: API smoke: start the api locally against the Spark 4 core, hit the health endpoint, run one load via the API**

- [ ] **Step 3: Cloud connector smokes (as available): BigQuery live suite (per-run datasets), Snowflake COPY smoke (already pending per project notes) — record results in the PR description**

### Task 18: Release train (in order; each step gates the next)

1. starlake-streaming: merge `spark4` → `main`, tag `v1.4.0`, publish per the GitHub Releases process.
2. starlake-core: flip `Versions.starlakeStreaming` to `1.4.0`, merge `spark4` → `master`, tag `v1.8.0`. This is a minor-version crossing: per project policy the docker image-version bump is manual — do it.
3. starlake-api: point `.versions` `SL_VERSION` at `1.8.0`, merge `spark4` → `main`.

### Task 19: Follow-ups (tracked, deliberately out of scope)

- **Elasticsearch**: watch elasticsearch-hadoop for the first release containing the merged Spark 4 PRs (#2546-#2548); then restore `esSpark212` in core `build.sbt`, un-`ignore` `ESLoadJobSpec`, verify against ES 9.
- **ai.starlake/spark-redshift fork**: rebuild against Spark 4.1, publish 7.x, restore the core test dependency and un-`ignore` the specs; update `SPARK_REDSHIFT_VERSION` in both `.versions` files and Setup.java.
- **json-schema-validator 1.4.0 → 2.0.4**: now unblocked by Jackson 2.21; requires `JsonSchemaFactory`/`SchemaValidatorsConfig` API migration.
- **Embrace ANSI mode**: remove the `sql.ansi.enabled=false` parity pin, fix ingestion/tests, document the behavior change.
- **Restore `-Xfatal-warnings`** in starlake-api once deprecation warnings are cleaned.
- **BigQuery connector**: swap `0.44.2-preview` for the first GA `spark-4.1-bigquery` release.

---

## Self-Review notes

- The exact Spark 4.1.3 signatures asserted here (classifyException arity, `JdbcUtils.getSchema`, `ClassicConversions` members, `XSDToSchema.read` overloads) were researched from release artifacts, but Step-level instructions say to confirm against the v4.1.3 tag when the compiler disagrees — compiler wins.
- The `spark-4.1-bigquery` artifact is `%` (no scala suffix) in BOTH repos — the old `%%` habit is the likeliest silent mistake.
- `hadoop-aws`/`hadoop-azure`/`hadoop-common` all move to 3.4.2 in BOTH repos; the api's azure was 3.4.1 and aws 3.3.6 — both change.
- Task ordering constraint: core Task 1-11 → streaming Task 12 (needs core publishLocal) → core Task 13 → api Tasks 14-16 (need core assembly).
