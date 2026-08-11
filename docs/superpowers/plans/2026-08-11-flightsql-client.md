# Arrow Flight SQL Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove all GizmoSQL support and make a connection with a URL like `jdbc:arrow-flight-sql://localhost:31338?useEncryption=true&disableCertificateVerification=true&tenant=acme&pool=bi&superuser=true` work as a regular Starlake JDBC connection.

**Architecture:** Flight SQL is a transport, not a dialect. `ConnectionInfo.getJdbcEngineName()` resolves the engine from the `dialect` connection option (default `duckdb`), so all downstream SQL generation reuses existing jdbcEngines machinery. The URL is otherwise opaque: TLS flags and QoD routing params (`tenant`, `pool`, `superuser`) pass through to the Arrow driver untouched. Local-DuckDB behaviors (client-side ATTACH rewrite, S3/home_directory session setup) are guarded off for Flight connections because they live server-side in the QoD/GizmoSQL model. The driver jar is a pure runtime artifact downloaded by Setup.java; there is no sbt dependency.

**Tech Stack:** Scala 2.13.18, SBT 1.11.5, JDK 17, Arrow Flight SQL JDBC driver 19.0.0 (runtime only).

**Spec:** `docs/superpowers/specs/2026-08-11-flightsql-client-design.md`

## Global Constraints

- JDK 17, SBT 1.11.5. `sbt compile` auto-formats with scalafmt.
- Tests run sequentially and forked: `sbt "testOnly *ClassName*"`.
- Clean break for GizmoSQL: no deprecated aliases, no compatibility shims.
- Flight SQL driver class: `org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver`. Driver version default in Setup.java: `19.0.0`.
- Flight SQL URL prefix: `jdbc:arrow-flight-sql:` (URL scheme segment `arrow-flight-sql`).
- Default dialect for Flight SQL connections: `duckdb`.
- `docs/` is gitignored at the top level; use `git add -f` for files under `docs/superpowers/`.
- Never use an em dash in any writing (docs, comments, CHANGELOG).

---

### Task 1: Remove GizmoSQL support

**Files:**
- Delete: `src/main/scala/ai/starlake/job/gizmo/` (whole directory: GizmoCmd.scala, GizmoConfig.scala, GizmoModels.scala, GizmoProcessClient.scala)
- Delete: `src/main/resources/reference-gizmo.conf`
- Modify: `src/main/scala/ai/starlake/job/Main.scala:19` (import) and `:132` (registration)
- Modify: `src/main/scala/ai/starlake/config/Settings.scala:221-224` (GizmoSql case class) and `:412` (AppConfig field)
- Modify: `src/main/resources/reference.conf:14` (include line)
- Modify: `src/main/resources/starlake.json:2338-2350` (GizmoV1 definition) and `:2735-2738` (gizmosql property)
- Modify: `src/test/scala/ai/starlake/schema/generator/YamlSerdeSpec.scala` (import at :14, arbitrary at :1303-1307, generator at :1401, constructor arg at :1490)
- Delete: `~/.claude/skills/gizmosql/` (user-level skill directory, outside the repo)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: an `AppConfig` without the `gizmosql` field. Later tasks must NOT reference `settings.appConfig.gizmosql`.

- [ ] **Step 1: Delete the gizmo package and reference conf**

```bash
rm -r src/main/scala/ai/starlake/job/gizmo
rm src/main/resources/reference-gizmo.conf
rm -rf ~/.claude/skills/gizmosql
```

- [ ] **Step 2: Remove the command registration from Main.scala**

Remove line 19:
```scala
import ai.starlake.job.gizmo.GizmoCmd
```
Remove the `GizmoCmd,` line from the command list (around line 132):
```scala
    JobCmd,
    GizmoCmd,      // <- delete this line
    QuackCmd,
```

- [ ] **Step 3: Remove GizmoSql from Settings.scala**

Delete the case class (around line 221):
```scala
  final case class GizmoSql(
    url: String,
    apiKey: String
  )
```
Delete the AppConfig field (around line 412):
```scala
    onExceptionRetries: Int,
    pythonLibsDir: String,
    gizmosql: GizmoSql          // <- delete; pythonLibsDir: String loses its trailing comma
    // createTableIfNotExists: Boolean
```
becomes:
```scala
    onExceptionRetries: Int,
    pythonLibsDir: String
    // createTableIfNotExists: Boolean
```

- [ ] **Step 4: Remove the include from reference.conf**

Delete line 14:
```
include "reference-gizmo"
```

- [ ] **Step 5: Remove the GizmoV1 schema from starlake.json**

Delete the whole `"GizmoV1": { ... },` object (lines 2338-2350, including the trailing comma after its closing brace) and the `gizmosql` property (lines 2735-2738):
```json
        "gizmosql": {
          "$ref": "#/definitions/GizmoV1",
          "description": "Gizmo server configuration"
        }
```
Watch the comma on the preceding property (`pythonLibsDir`) so the JSON stays valid. Verify with:
```bash
python3 -m json.tool src/main/resources/starlake.json > /dev/null && echo JSON_OK
```
Expected: `JSON_OK`

- [ ] **Step 6: Remove GizmoSql from YamlSerdeSpec.scala**

Remove `GizmoSql,` from the import block (line 14). Delete the arbitrary (lines 1303-1307):
```scala
  implicit val gizmo: Arbitrary[GizmoSql] = Arbitrary {
    for {
      url    <- arbitrary[String]
      apiKey <- arbitrary[String]
    } yield GizmoSql(url = url, apiKey = apiKey)
  }
```
Delete the generator line (around 1401): `gizmosql                <- arbitrary[GizmoSql]`
Delete the constructor arg (around 1490): `gizmosql = gizmosql` (and the trailing comma on the previous line `pythonLibsDir = pythonLibsDir`).

- [ ] **Step 7: Compile and run the serde test**

```bash
sbt compile
sbt "testOnly *YamlSerdeSpec*"
```
Expected: compile succeeds; YamlSerdeSpec passes.

- [ ] **Step 8: Verify nothing references gizmo anymore**

```bash
grep -rin gizmo src/main src/test build.sbt project/ | grep -v -i "target"
```
Expected: no output.

- [ ] **Step 9: Commit**

```bash
git add -A src/main src/test
git commit -m "feat!: remove gizmosql command and settings (breaking change)"
```

---

### Task 2: ConnectionInfo Flight SQL support (TDD)

**Files:**
- Create: `src/test/scala/ai/starlake/config/ConnectionInfoFlightSqlSpec.scala`
- Modify: `src/main/scala/ai/starlake/config/ConnectionInfo.scala`

**Interfaces:**
- Consumes: nothing from other tasks (pure model change).
- Produces, all on `ConnectionInfo`, used by Tasks 3 and 4:
  - `def isFlightSql(): Boolean` (true when `options("url")` starts with `jdbc:arrow-flight-sql:`)
  - `getJdbcEngineName(): Engine` returns the engine of `options.getOrElse("dialect", "duckdb")` for Flight SQL connections
  - `dialect: JdbcDialect` resolved against a copy of the URL with the scheme rewritten to the resolved engine; the stored `url` option is never modified

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/ai/starlake/config/ConnectionInfoFlightSqlSpec.scala`:
```scala
package ai.starlake.config

import ai.starlake.schema.model.ConnectionType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConnectionInfoFlightSqlSpec extends AnyFlatSpec with Matchers {

  private val qodUrl =
    "jdbc:arrow-flight-sql://localhost:31338?useEncryption=true" +
      "&disableCertificateVerification=true&tenant=acme&pool=bi&superuser=true"

  private def flightConnection(extraOptions: Map[String, String] = Map.empty): ConnectionInfo =
    ConnectionInfo(
      `type` = ConnectionType.JDBC,
      options = Map(
        "url"    -> qodUrl,
        "driver" -> "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
      ) ++ extraOptions
    )

  "a flight sql connection" should "be detected by isFlightSql" in {
    flightConnection().isFlightSql() shouldBe true
  }

  "a plain jdbc connection" should "not be detected by isFlightSql" in {
    val conn = ConnectionInfo(
      `type` = ConnectionType.JDBC,
      options =
        Map("url" -> "jdbc:postgresql://localhost:5432/db", "driver" -> "org.postgresql.Driver")
    )
    conn.isFlightSql() shouldBe false
  }

  "engine resolution" should "default to duckdb for flight sql connections" in {
    flightConnection().getJdbcEngineName().toString shouldBe "duckdb"
    flightConnection().isDuckDb() shouldBe true
  }

  it should "honor the dialect option" in {
    val conn = flightConnection(Map("dialect" -> "postgresql"))
    conn.getJdbcEngineName().toString shouldBe "postgresql"
    conn.isPostgreSql() shouldBe true
    conn.isDuckDb() shouldBe false
  }

  "targetDatawareHouse" should "return the dialect for flight sql connections" in {
    flightConnection().targetDatawareHouse() shouldBe "duckdb"
    flightConnection(Map("dialect" -> "postgresql")).targetDatawareHouse() shouldBe "postgresql"
  }

  "getDatabaseName" should "come from the database option for flight sql connections" in {
    flightConnection(Map("database" -> "lake")).getDatabaseName() shouldBe Some("lake")
    flightConnection(Map("db" -> "lake2")).getDatabaseName() shouldBe Some("lake2")
    flightConnection().getDatabaseName() shouldBe None
  }

  "spark dialect resolution" should "use the rewritten scheme" in {
    // the postgresql dialect registered in spark quotes with double quotes
    val conn = flightConnection(Map("dialect" -> "postgresql"))
    conn.quoteIdentifier("x") shouldBe "\"x\""
  }

  "the qod gateway url" should "keep its query string untouched" in {
    val conn = flightConnection()
    conn.jdbcUrl shouldBe qodUrl
    conn.jdbcUrl should include("tenant=acme")
    conn.jdbcUrl should include("pool=bi")
    conn.jdbcUrl should include("superuser=true")
  }

  "isMotherDuckDb" should "be false for flight sql connections" in {
    flightConnection().isMotherDuckDb() shouldBe false
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
sbt "testOnly *ConnectionInfoFlightSqlSpec*"
```
Expected: FAIL. `isFlightSql` is not a member of `ConnectionInfo` (compile error).

- [ ] **Step 3: Implement in ConnectionInfo.scala**

Add near the other `isXxx` helpers (after `isDucklake()` around line 35):
```scala
  @JsonIgnore
  def isFlightSql(): Boolean =
    options.get("url").exists(_.startsWith("jdbc:arrow-flight-sql:"))

  @JsonIgnore
  private def flightSqlDialect(): String =
    options.getOrElse("dialect", "duckdb").toLowerCase()
```

In `getJdbcEngineName()` (around line 437), the method ends with `Engine.fromString(engineName)`. Replace that last expression with:
```scala
    val resolvedEngineName =
      if (engineName.toLowerCase() == "arrow-flight-sql") flightSqlDialect()
      else engineName
    Engine.fromString(resolvedEngineName)
```

In `targetDatawareHouse()` (around line 384), inside `case ConnectionType.JDBC`, add a flight branch before the generic url parse:
```scala
      case ConnectionType.JDBC =>
        if (options.contains("sfUrl"))
          options("sfUrl").split(':')(1).toLowerCase() // should return snowflake
        else if (isFlightSql())
          getJdbcEngineName().toString
        else if (options.contains("url")) {
          options("url").split(':')(1).toLowerCase()
        } else "spark"
```

In `getDatabaseName()` (around line 415), add a first case to the match on `this.getJdbcEngineName().toString`:
```scala
    this.getJdbcEngineName().toString match {
      case _ if isFlightSql() =>
        options.get("database").orElse(options.get("db"))
      case "duckdb" =>
```

Change the `dialect` lazy val (around line 512) from:
```scala
  lazy val dialect: JdbcDialect =
    applyIfConnectionTypeIs(ConnectionType.JDBC, SparkUtils.dialectForUrl(jdbcUrl))
```
to:
```scala
  lazy val dialect: JdbcDialect =
    applyIfConnectionTypeIs(
      ConnectionType.JDBC, {
        val urlForDialect =
          if (isFlightSql())
            jdbcUrl.replaceFirst(
              "^jdbc:arrow-flight-sql:",
              s"jdbc:${getJdbcEngineName().toString}:"
            )
          else jdbcUrl
        SparkUtils.dialectForUrl(urlForDialect)
      }
    )
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
sbt "testOnly *ConnectionInfoFlightSqlSpec*"
```
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/config/ConnectionInfo.scala src/test/scala/ai/starlake/config/ConnectionInfoFlightSqlSpec.scala
git commit -m "feat: resolve flight sql connections to their dialect engine"
```

---

### Task 3: Default the driver in Settings normalization (TDD)

**Files:**
- Create: `src/test/scala/ai/starlake/config/FlightSqlSettingsSpec.scala`
- Modify: `src/main/scala/ai/starlake/config/Settings.scala` (`adjustConnectionProperties`, around line 1072)

**Interfaces:**
- Consumes: `isFlightSql()`, `isDuckDb()` from Task 2.
- Produces: after `Settings.adjustConnectionProperties`, Flight SQL connections always carry a `driver` option (default `org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver`) and have `sparkFormat = None` when the dialect is duckdb. `StarlakeConnectionPool.getConnection` requires the `driver` option, so this is what makes a driver-less YAML connection work.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/ai/starlake/config/FlightSqlSettingsSpec.scala`:
```scala
package ai.starlake.config

import ai.starlake.TestHelper
import ai.starlake.schema.model.ConnectionType

class FlightSqlSettingsSpec extends TestHelper {

  private val qodUrl =
    "jdbc:arrow-flight-sql://localhost:31338?useEncryption=true" +
      "&disableCertificateVerification=true&tenant=acme&pool=bi&superuser=true"

  new WithSettings() {
    "flight sql connection normalization" should "fill in the default driver and strip sparkFormat for duckdb dialect" in {
      val conn = ConnectionInfo(
        `type` = ConnectionType.JDBC,
        sparkFormat = Some("jdbc"),
        options = Map("url" -> qodUrl)
      )
      val appConfig = settings.appConfig.copy(connections = Map("qod" -> conn))
      val adjusted = Settings.adjustConnectionProperties(settings.copy(appConfig = appConfig))
      val adjustedConn = adjusted.appConfig.connections("qod")
      adjustedConn.options("driver") shouldBe "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
      adjustedConn.sparkFormat shouldBe None
      adjustedConn.options("url") shouldBe qodUrl // url untouched
    }

    it should "not override an explicit driver and keep sparkFormat for non-duckdb dialects" in {
      val conn = ConnectionInfo(
        `type` = ConnectionType.JDBC,
        sparkFormat = Some("jdbc"),
        options = Map(
          "url"     -> qodUrl,
          "driver"  -> "com.example.CustomFlightDriver",
          "dialect" -> "postgresql"
        )
      )
      val appConfig = settings.appConfig.copy(connections = Map("qod" -> conn))
      val adjusted = Settings.adjustConnectionProperties(settings.copy(appConfig = appConfig))
      val adjustedConn = adjusted.appConfig.connections("qod")
      adjustedConn.options("driver") shouldBe "com.example.CustomFlightDriver"
      adjustedConn.sparkFormat shouldBe Some("jdbc")
    }
  }
}
```
Note: `adjustConnectionProperties` is the public method in `object Settings` containing the `connection.isSnowflake()` / `connection.isDuckDb()` normalization chain. If it is not applied automatically on settings load in the test path, calling it directly as above is the contract being tested.

- [ ] **Step 2: Run the test to verify it fails**

```bash
sbt "testOnly *FlightSqlSettingsSpec*"
```
Expected: FAIL on the driver assertion (key `driver` not found).

- [ ] **Step 3: Implement in Settings.adjustConnectionProperties**

Insert a flight branch before the `isDuckDb()` branch (around line 1083):
```scala
          } else if (connection.isFlightSql()) {
            val withDriver =
              if (connection.options.contains("driver")) connection.options
              else
                connection.options +
                ("driver" -> "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")
            val newSparkFormat =
              if (connection.isDuckDb()) None // duckdb dialect: spark mode not supported
              else connection.sparkFormat
            connection.copy(options = withDriver, sparkFormat = newSparkFormat)
          } else if (connection.isDuckDb())
            connection.copy(sparkFormat = None) // spark mode not supported in duckdb
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
sbt "testOnly *FlightSqlSettingsSpec*"
```
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/config/Settings.scala src/test/scala/ai/starlake/config/FlightSqlSettingsSpec.scala
git commit -m "feat: default the arrow flight sql jdbc driver on flight connections"
```

---

### Task 4: Keep Flight connections fully remote

**Files:**
- Modify: `src/main/scala/ai/starlake/extract/StarlakeConnectionPool.scala:119-129`
- Modify: `src/main/scala/ai/starlake/extract/JdbcDbUtils.scala` (`runDuckLakePreActions`, around line 188)

**Interfaces:**
- Consumes: the `jdbc:arrow-flight-sql:` URL prefix convention (Task 2). These two files receive raw option maps, not `ConnectionInfo`, so they test the URL prefix directly.
- Produces: Flight SQL connections always take the HikariCP path with their own URL (never rewritten to local `jdbc:duckdb:`); local DuckDB session setup (home_directory, secret_directory, S3 SETs) never runs on them. Generic `preActions`/`postActions` still execute remotely.

- [ ] **Step 1: Guard the attach-backed rewrite in StarlakeConnectionPool**

Current code (`getConnection`, lines 119-123):
```scala
    val isAttachBacked =
      connectionOptions
        .get("preActions")
        .exists(pa => pa.contains("ducklake:") || pa.contains("quack:"))
```
Replace with:
```scala
    // Flight SQL connections are pure remote clients: any ducklake/quack attach
    // happens server-side, never on this JVM (see docs/quack.md isolation model)
    val isFlightSql =
      connectionOptions.get("url").exists(_.startsWith("jdbc:arrow-flight-sql:"))
    val isAttachBacked =
      !isFlightSql &&
      connectionOptions
        .get("preActions")
        .exists(pa => pa.contains("ducklake:") || pa.contains("quack:"))
```

- [ ] **Step 2: Guard local session setup in runDuckLakePreActions**

In `JdbcDbUtils.runDuckLakePreActions` (line 188), after `val isDucklake = ...`, add:
```scala
    val isFlightSql =
      connectionOptions.get("url").exists(_.startsWith("jdbc:arrow-flight-sql:"))
```
Then wrap the three local-setup blocks (the `SL_DUCKDB_HOME` Try, the `SL_DUCKDB_SECRET_HOME` Try, and the `fs.s3a.endpoint` foreach) in a single guard:
```scala
    if (!isFlightSql) {
      // ... the three existing blocks, unchanged ...
    }
```
Leave the trailing `preActions.foreach { actions => ... }` loop outside the guard so session SQL (USE, SET schema) still runs over the Flight connection.

- [ ] **Step 3: Compile and run the flight suites**

```bash
sbt "testOnly *ConnectionInfoFlightSqlSpec* *FlightSqlSettingsSpec*"
```
Expected: PASS, no regressions.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/ai/starlake/extract/StarlakeConnectionPool.scala src/main/scala/ai/starlake/extract/JdbcDbUtils.scala
git commit -m "feat: keep flight sql connections fully remote, skip local ducklake setup"
```

---

### Task 5: Setup.java downloads the driver

**Files:**
- Modify: `src/main/java/Setup.java`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `setup.jar` downloads `flight-sql-jdbc-driver-19.0.0.jar` into `bin/deps` when `ENABLE_FLIGHTSQL` is on (default true), version overridable via `FLIGHT_SQL_JDBC_VERSION`.

- [ ] **Step 1: Add the enabler flag**

After `ENABLE_DUCKDB` (line 216):
```java
    public static boolean ENABLE_FLIGHTSQL = ENABLE_ALL || envIsTrueWithDefaultTrue("ENABLE_FLIGHTSQL");
```
Add `ENABLE_FLIGHTSQL,` to the `getAllEnablers()` array (after `ENABLE_DUCKDB,`). Add `ENABLE_FLIGHTSQL` to the javadoc env var list on line 48.

- [ ] **Step 2: Add the version and resource**

Near the DUCKDB constants (line 322):
```java
    // ARROW FLIGHT SQL
    private static final String FLIGHT_SQL_JDBC_VERSION = getEnv("FLIGHT_SQL_JDBC_VERSION").orElse("19.0.0");
```
Near `DUCKDB_JAR` (line 365):
```java
    private static final ResourceDependency FLIGHT_SQL_JDBC_JAR = new ResourceDependency("flight-sql-jdbc-driver", "https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/" + FLIGHT_SQL_JDBC_VERSION + "/flight-sql-jdbc-driver-" + FLIGHT_SQL_JDBC_VERSION + ".jar");
```
Near `duckDbDependencies` (line 402):
```java
    private static final ResourceDependency[] flightSqlDependencies = {
            FLIGHT_SQL_JDBC_JAR
    };
```

- [ ] **Step 3: Wire env var generation**

In the versions writer (around lines 500-545): after the `ENABLE_DUCKDB` line add
```java
            variableWriter.apply(writer).accept("ENABLE_FLIGHTSQL", String.valueOf(ENABLE_FLIGHTSQL));
```
and after the `if (ENABLE_DUCKDB || !anyDependencyEnabled())` block add
```java
            if (ENABLE_FLIGHTSQL || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("FLIGHT_SQL_JDBC_VERSION", FLIGHT_SQL_JDBC_VERSION);
            }
```

- [ ] **Step 4: Wire the download**

In the download sequence (around line 933), after the duckdb block:
```java
            deleteDependencies(flightSqlDependencies, depsDir);
            if (ENABLE_FLIGHTSQL) {
                downloadAndDisplayProgress(flightSqlDependencies, depsDir, true);
            }
```

- [ ] **Step 5: Wire the interactive menus**

`enableAllDependencies()` (line 645): add `ENABLE_FLIGHTSQL = true;`.

Unix menu (`askUserWhichConfigToEnableUnix`): insert after the DuckDB row and shift everything below by one (the menu then has indices 0-13):
```java
                printMenuOption(5, currentSelection, ENABLE_DUCKDB, "DuckDB");
                printMenuOption(6, currentSelection, ENABLE_FLIGHTSQL, "FlightSQL");
                printMenuOption(7, currentSelection, ENABLE_SPARK , "Spark");
                printMenuOption(8, currentSelection, ENABLE_KAFKA, "Kafka");
                printMenuOption(9, currentSelection, ENABLE_MARIADB, "Mariadb");
                printMenuOption(10, currentSelection, ENABLE_TRINODB, "Trino");
```
Update the fixed rows and bounds: `Select All` compares to 11, `Select None` to 12, `DONE` to 13; the UP wraparound sets `currentSelection = 13`, the DOWN check is `> 13`; the toggle condition becomes `currentSelection <= 10`; `currentSelection == 11` is All, `== 12` is None (add `ENABLE_FLIGHTSQL = false;` to its reset list), `== 13` is Done. Update the index comment above the loop. In `toggleOption`, insert `case 6: ENABLE_FLIGHTSQL = !ENABLE_FLIGHTSQL; break;` and shift Spark/Kafka/Mariadb/Trino to cases 7/8/9/10.

Windows menu (`askUserWhichConfigToEnableWindows`): add `System.out.println((ENABLE_FLIGHTSQL ? "[x]" : "[ ]") + " FlightSQL");` after the DuckDB line, add `ENABLE_FLIGHTSQL = false;` to the `"n"` reset branch, and renumber the switch: `case "7": ENABLE_FLIGHTSQL = !ENABLE_FLIGHTSQL; break;`, `case "8":` spark noop (empty), `case "9":` Kafka, `case "10":` Mariadb, `case "11":` Trino. Add `ENABLE_FLIGHTSQL` to the env var list printed at line 869.

- [ ] **Step 6: Compile**

```bash
sbt compile
```
Expected: succeeds (Setup.java is part of the main source set).

- [ ] **Step 7: Commit**

```bash
git add src/main/java/Setup.java
git commit -m "feat: setup downloads arrow flight sql jdbc driver (ENABLE_FLIGHTSQL)"
```

---

### Task 6: CHANGELOG and full verification

**Files:**
- Modify: `CHANGELOG.md` (current `1.5.16-SNAPSHOT` section)

**Interfaces:**
- Consumes: everything above.
- Produces: release notes; a green build.

- [ ] **Step 1: Add CHANGELOG entries**

In the `# 1.5.16-SNAPSHOT:` section, under `__Improvement__:` add:
```markdown
- **Arrow Flight SQL client**: Any Flight SQL endpoint (quack-on-demand gateway, GizmoSQL, Dremio, ...) is now a regular Starlake JDBC connection: `url: "jdbc:arrow-flight-sql://host:port?useEncryption=true&tenant=acme&pool=bi"`. The query string is passed to the Arrow driver untouched (TLS flags consumed by the driver, gateway routing params forwarded to the server). The driver class defaults to the official Arrow Flight SQL JDBC driver, downloaded by setup when `ENABLE_FLIGHTSQL` is on (version via `FLIGHT_SQL_JDBC_VERSION`). Flight SQL is a transport: the SQL dialect of the backing engine is chosen with the `dialect` connection option (default `duckdb`). The client stays fully remote: client-side ducklake/quack ATTACH rewriting and local DuckDB session setup (S3 secrets, home_directory) are skipped for Flight connections.
```
Under `__Breaking change__:` replace the existing `gizmosql stop` entry with:
```markdown
- **`gizmosql` command removed**: The GizmoSQL process-manager integration (`starlake gizmosql start|stop|list|stop-all`, the `gizmosql.url` / `gizmosql.apiKey` settings and `SL_GIZMO_URL` / `SL_GIZMO_API_KEY` env vars) is gone. Expose DuckDB/DuckLake through any Arrow Flight SQL server and connect with a `jdbc:arrow-flight-sql://` connection instead.
```

- [ ] **Step 2: Full verification**

```bash
sbt scalafmtCheck compile
sbt "testOnly *ConnectionInfoFlightSqlSpec* *FlightSqlSettingsSpec* *YamlSerdeSpec*"
```
Expected: format check clean, compile clean, all three suites pass. Then run the full suite if time allows:
```bash
sbt test
```
Expected: PASS (long; sequential and forked).

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: changelog for gizmosql removal and flight sql client"
```

- [ ] **Step 4: Manual end-to-end check (requires a live QoD gateway)**

With a quack-on-demand gateway running locally and the driver jar in `bin/deps`, declare the connection in a test project and run any command that opens it, for example:
```bash
starlake settings test --connection qod_bi
```
using:
```yaml
connections:
  qod_bi:
    type: jdbc
    options:
      url: "jdbc:arrow-flight-sql://localhost:31338?useEncryption=true&disableCertificateVerification=true&tenant=acme&pool=bi&superuser=true"
      user: "..."
      password: "..."
```
Expected: connection succeeds and queries route through the gateway. This step is manual and outside CI.
