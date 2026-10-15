package ai.starlake.serve.catalog

import ai.starlake.TestHelper

class CatalogCommandsSpec extends TestHelper {

  private lazy val (_, connections) = DuckDbCatalogFixture.create()

  private def run(args: String*)(implicit settings: ai.starlake.config.Settings): String =
    CatalogCommands.handle(args.toArray, new CatalogService(connections))

  "isCatalogCommand" should "recognize catalog-* commands only" in {
    CatalogCommands.isCatalogCommand("catalog-schemas") shouldBe true
    CatalogCommands.isCatalogCommand("table-names") shouldBe false
  }

  "handle" should "answer catalog-connections with the envelope" in {
    new WithSettings() {
      val json = run("catalog-connections")
      json should startWith("""{"ok":true,"data":[{"name":"broken"""")
    }
  }

  it should "answer catalog-tables" in {
    new WithSettings() {
      run("catalog-tables", "--connection", "duck", "--schema", "s_les") shouldBe
      """{"ok":true,"data":[{"name":"other","kind":"TABLE","remarks":null}]}"""
    }
  }

  it should "use the default preview limit and parse --limit" in {
    new WithSettings() {
      run(
        "catalog-preview",
        "--connection",
        "duck",
        "--schema",
        "sales",
        "--table",
        "orders"
      ) should
      include(""""truncated":false""")
      run(
        "catalog-preview",
        "--connection",
        "duck",
        "--schema",
        "sales",
        "--table",
        "orders",
        "--limit",
        "1"
      ) should
      include(""""truncated":true""")
    }
  }

  it should "report BAD_REQUEST for a missing argument" in {
    new WithSettings() {
      run("catalog-tables", "--connection", "duck") shouldBe
      """{"ok":false,"error":{"code":"BAD_REQUEST","message":"Missing argument --schema"}}"""
    }
  }

  it should "report BAD_REQUEST for a non-numeric limit" in {
    new WithSettings() {
      run(
        "catalog-preview",
        "--connection",
        "duck",
        "--schema",
        "sales",
        "--table",
        "orders",
        "--limit",
        "ten"
      ) should
      include(""""code":"BAD_REQUEST"""")
    }
  }

  it should "report UNKNOWN_CONNECTION through the envelope" in {
    new WithSettings() {
      run("catalog-schemas", "--connection", "nope") should include(
        """"code":"UNKNOWN_CONNECTION""""
      )
    }
  }

  it should "turn an unexpected failure into a scrubbed QUERY_FAILED" in {
    new WithSettings() {
      val leaking = new CatalogService(connections) {
        override protected def backend(name: String): CatalogBackend =
          throw new RuntimeException(s"boom with ${DuckDbCatalogFixture.BrokenPassword}")
      }
      val json = CatalogCommands.handle(
        Array("catalog-schemas", "--connection", "duck"),
        leaking
      )
      json should include(""""code":"QUERY_FAILED"""")
      json should include("boom with ***")
      json should not include DuckDbCatalogFixture.BrokenPassword
    }
  }

  it should "scrub the passwords of every connection, even when they share an option key" in {
    new WithSettings() {
      val first = "first-pw-aaa"
      val second = "second-pw-bbb"
      val two = Map(
        "one" -> ai.starlake.config.ConnectionInfo(
          ai.starlake.schema.model.ConnectionType.JDBC,
          options = Map("url" -> "jdbc:x://a", "password" -> first)
        ),
        "two" -> ai.starlake.config.ConnectionInfo(
          ai.starlake.schema.model.ConnectionType.JDBC,
          options = Map("url" -> "jdbc:x://b", "password" -> second)
        )
      )
      val leaking = new CatalogService(two) {
        override protected def backend(name: String): CatalogBackend =
          throw new RuntimeException(s"boom $first and $second")
      }
      val json = CatalogCommands.handle(
        Array("catalog-schemas", "--connection", "one"),
        leaking
      )
      json should include(""""code":"QUERY_FAILED"""")
      json should not include first
      json should not include second
    }
  }
}
