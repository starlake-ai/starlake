# Remove GizmoSQL, add Arrow Flight SQL client support

Date: 2026-08-11
Status: Approved

## Goal

Remove all GizmoSQL server-management support from Starlake and replace the Arrow
Flight SQL story with first-class client support: any Flight SQL endpoint (DuckDB,
Dremio, Doris, DuckLake gateways, ...) becomes usable as a regular Starlake JDBC
connection with full engine parity (extract-schema, extract-data, load sink,
transform target, audit and expectation tables).

## Decisions

- Full engine parity, not source-only.
- Dialect is chosen by the connection option `dialect`, defaulting to `duckdb`.
- Driver is the official `org.apache.arrow:flight-sql-jdbc-driver`, a pure
  runtime artifact downloaded by `Setup.java` (same delivery as
  Snowflake/Redshift/Postgres); no sbt dependency.
- Clean break: the `gizmosql` command is removed with no deprecated alias.

## Part 1: GizmoSQL removal

Delete outright:

- `src/main/scala/ai/starlake/job/gizmo/` (GizmoCmd, GizmoConfig, GizmoModels,
  GizmoProcessClient)
- `GizmoCmd` import and registration in `job/Main.scala`
- `GizmoSql` case class (`Settings.scala:221`) and the `gizmosql` field in
  `AppConfig` (`Settings.scala:412`)
- `src/main/resources/reference-gizmo.conf` and the `include "reference-gizmo"`
  line in `reference.conf`
- `GizmoV1` definition and the `gizmosql` property reference in
  `src/main/resources/starlake.json`
- `GizmoSql` arbitrary, import, and `gizmosql` field usage in
  `src/test/scala/ai/starlake/schema/generator/YamlSerdeSpec.scala`
- The user-level skill directory `~/.claude/skills/gizmosql`

Untouched: CHANGELOG history, `docs/superpowers/` historical plans/specs, and all
Quack code (`job/quack/`, `isQuackClient`/`isQuackServer` in `ConnectionInfo`),
which is a separate feature.

## Part 2: Connection model and dialect resolution

A Flight SQL connection is ordinary JDBC YAML:

```yaml
connections:
  my_flight:
    type: jdbc
    options:
      url: "jdbc:arrow-flight-sql://host:32010/?useEncryption=true"
      driver: "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"  # optional, defaulted
      user: "..."
      password: "..."
      dialect: "duckdb"   # optional, defaults to duckdb
```

### Gateway URLs (quack-on-demand)

Everything after the host:port is opaque to Starlake: the URL query string is
passed to the Arrow driver untouched, and the driver forwards unknown
parameters to the server. Gateway-specific routing therefore needs no Starlake
support. Connecting to a quack-on-demand FlightSQL gateway looks like:

```yaml
connections:
  qod_bi:
    type: jdbc
    options:
      url: "jdbc:arrow-flight-sql://localhost:31338?useEncryption=true&disableCertificateVerification=true&tenant=acme&pool=bi&superuser=true"
      user: "..."
      password: "..."
      # dialect defaults to duckdb, which is what QoD fronts
```

`useEncryption` and `disableCertificateVerification` are TLS flags understood
by the Arrow driver itself; `tenant`, `pool`, and `superuser` are QoD routing
parameters the driver passes through to the gateway. Starlake only ever
inspects the `jdbc:arrow-flight-sql:` scheme (dialect resolution rewrites the
scheme purely for the Spark `JdbcDialects` quoting lookup; the connection
always uses the original URL). Connections with different query strings (other
tenants or pools) get distinct HikariCP pools because the pool key hashes the
full URL.

Flight SQL is a transport, not a dialect: the endpoint fronts a real engine whose
SQL Starlake must generate. Changes, all in `config/ConnectionInfo.scala`:

- `getJdbcEngineName()`: when the JDBC URL scheme segment is `arrow-flight-sql`,
  return `Engine.fromString(options.getOrElse("dialect", "duckdb"))` instead of
  the scheme. Every downstream consumer (jdbcEngines templates, audit and
  expectation DDL, merge strategy SQL, `isDuckDb()` and friends) then behaves as
  if talking to the backing engine natively.
- `dialect` (the Spark `JdbcDialect` lazy val): for `arrow-flight-sql` URLs,
  rewrite the scheme to the resolved dialect engine (e.g.
  `jdbc:arrow-flight-sql:...` -> `jdbc:duckdb:...`) before the
  `JdbcDialects.get` lookup so identifier quoting matches the backing engine.
- New `isFlightSql(): Boolean` helper (URL scheme check) for transport-specific
  spots.
- Driver defaulting: when the URL is `arrow-flight-sql` and no `driver` option is
  present, default to `org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver`.
- `targetDatawareHouse()` and `getDatabaseName()` get the same dialect
  interception where they parse the URL scheme.

No new `jdbcEngines` entry is added; dialect delegation reuses the existing
entries (spark, bigquery, snowflake, duckdb, postgresql, redshift, sqlserver,
mysql, mariadb).

## Part 2b: DuckDB / DuckLake behind Flight SQL (primary scenario)

The default dialect is `duckdb` because the primary deployment model is a
DuckDB or DuckLake lakehouse exposed through a Flight SQL server, exactly the
way quack-on-demand and GizmoSQL work: the server attaches the DuckLake and
holds the object-storage credentials; the client is a pure remote participant
that never runs `ATTACH 'ducklake:...'` and never sees Parquet files or
secrets (same isolation model as `docs/quack.md`).

Consequences:

- `StarlakeConnectionPool.getConnection`: the attach-backed special case
  (`preActions` containing `ducklake:` or `quack:` forces a local
  `jdbc:duckdb:` URL) must never fire for `arrow-flight-sql` URLs. Flight SQL
  connections always take the standard HikariCP path with their own URL.
- `JdbcDbUtils.runDuckLakePreActions`: the local-DuckDB session setup (SET
  `home_directory`, `secret_directory`, S3 endpoint/credential SETs) is
  skipped for Flight SQL connections; credentials live server-side. The
  generic `preActions` loop still runs, so session SQL such as `USE lake` or
  `SET schema` can be sent over the Flight connection.
- Loading: `IngestionJob.selectLoader()` resolves `duckdb` for
  dialect-duckdb Flight connections, so `DuckDbNativeLoader` runs its
  `read_csv`/`read_json` SQL over the remote connection, server-side. Files
  must therefore be visible to the server (object storage the server has
  secrets for, or a shared filesystem). Purely client-local file paths fail
  with the server's error; this is documented, not worked around.
- `Settings` connection normalization strips `sparkFormat` for
  dialect-duckdb Flight connections (same rule as native DuckDB) and fills in
  the default driver class.

## Part 3: Connection pool and Spark integration

- `StarlakeConnectionPool`: no structural change. Flight SQL connections use the
  normal HikariCP path with the driver class from options.
- Spark read/write paths use the existing `format("jdbc")` machinery with the
  same URL and driver options; the driver jar reaches the Spark classpath via
  the Setup.java deps directory like other provided drivers.

## Part 4: Packaging and Setup.java

- No sbt dependency at all: Starlake never references driver classes at compile
  time (the driver is loaded by class name through the normal JDBC machinery),
  so the jar is a pure runtime artifact delivered by Setup.java. Version
  `19.0.0` (latest stable on Maven Central as of 2026-08-11).
- `src/main/java/Setup.java`:
  - `ENABLE_FLIGHTSQL` flag, default true via `envIsTrueWithDefaultTrue`, honored
    by `ENABLE_ALL` and the interactive prompt like the other flags.
  - `FLIGHT_SQL_JDBC_VERSION` env override, default `19.0.0`.
  - `ResourceDependency` downloading
    `https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/<v>/flight-sql-jdbc-driver-<v>.jar`.
- JVM flags: nothing to add. `--add-opens=java.base/java.nio=ALL-UNNAMED` is
  already set in `build.sbt` test options and `distrib/starlake.sh`.

## Part 5: Testing

- Unit tests (new spec, e.g. `ConnectionInfoFlightSqlSpec`):
  - `arrow-flight-sql` URL without `dialect` resolves engine to `duckdb`.
  - `dialect: postgresql` resolves engine to `postgresql`.
  - Driver option defaults to the Arrow driver class when absent.
  - Spark dialect lookup uses the rewritten scheme.
  - The full QoD gateway URL
    (`jdbc:arrow-flight-sql://localhost:31338?useEncryption=true&disableCertificateVerification=true&tenant=acme&pool=bi&superuser=true`)
    resolves and is never rewritten.
- End-to-end verification is manual, against a live QoD gateway with the URL
  above. No in-process Flight SQL test server is bundled.
- `YamlSerdeSpec` updated for the removed `GizmoSql` settings type.

## Part 6: Docs, skills, changelog

- Delete `~/.claude/skills/gizmosql`.
- CHANGELOG entry: breaking removal of the `gizmosql` command; new Arrow Flight
  SQL client support with the YAML example from Part 2.

## Error handling

- Unknown `dialect` value: the existing "engine not found" error path applies
  at first use (no dedicated validation).
- Missing driver jar (user skipped Setup or ENABLE_FLIGHTSQL=false): the pool's
  existing driver-class-not-found failure surfaces; the message already names the
  missing class, which is enough to point at Setup.
