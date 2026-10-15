package ai.starlake.serve.catalog

import ai.starlake.TestHelper

class CatalogServiceSpec extends TestHelper {

  private lazy val (_, connections) = DuckDbCatalogFixture.create()

  private def service(implicit settings: ai.starlake.config.Settings) =
    new CatalogService(connections)

  private def serviceWithActive(active: Option[String])(implicit
    settings: ai.starlake.config.Settings
  ) = new CatalogService(connections, active)

  "listConnections" should "list every connection sorted, marking FS as not browsable" in {
    new WithSettings() {
      val result = service.listConnections()
      result.map(_.name) shouldBe List("broken", "duck", "lake")
      val duck = result.find(_.name == "duck").get
      duck.browsable shouldBe true
      duck.engine shouldBe "duckdb"
      duck.`type` shouldBe "JDBC"
      val lake = result.find(_.name == "lake").get
      lake.browsable shouldBe false
      lake.reason shouldBe Some("Spark and filesystem connections cannot be browsed")
    }
  }

  it should "mark only the active connection and report sorted masked details" in {
    new WithSettings() {
      val result = serviceWithActive(Some("duck")).listConnections()
      result.filter(_.active).map(_.name) shouldBe List("duck")
      val broken = result.find(_.name == "broken").get
      broken.details("password") shouldBe "***"
      broken.details.values.exists(_.contains(DuckDbCatalogFixture.BrokenPassword)) shouldBe false
      broken.details.keys.toList shouldBe broken.details.keys.toList.sorted
    }
  }

  it should "mark nothing active without an active connection" in {
    new WithSettings() {
      serviceWithActive(None).listConnections().exists(_.active) shouldBe false
      serviceWithActive(Some("nope")).listConnections().exists(_.active) shouldBe false
    }
  }

  it should "mask a password embedded in a JDBC url detail" in {
    new WithSettings() {
      val info = ai.starlake.config.ConnectionInfo(
        ai.starlake.schema.model.ConnectionType.JDBC,
        options = Map("url" -> "jdbc:postgresql://h/db?user=bob&password=abc123")
      )
      val result = new CatalogService(Map("pg" -> info)).listConnections()
      result.head.details("url") shouldBe "jdbc:postgresql://h/db?user=bob&password=***"
    }
  }

  "listSchemas" should "list schemas without INFORMATION_SCHEMA" in {
    new WithSettings() {
      val names = service.listSchemas("duck").map(_.name)
      names should contain allOf ("main", "sales", "s_les")
      names.map(_.toUpperCase) should not contain "INFORMATION_SCHEMA"
      names shouldBe names.distinct
    }
  }

  it should "fail with UNKNOWN_CONNECTION for an undeclared connection" in {
    new WithSettings() {
      val e = intercept[CatalogException](service.listSchemas("nope"))
      e.code shouldBe CatalogErrorCode.UnknownConnection
    }
  }

  it should "fail with NOT_BROWSABLE for a filesystem connection" in {
    new WithSettings() {
      intercept[CatalogException](service.listSchemas("lake")).code shouldBe
      CatalogErrorCode.NotBrowsable
    }
  }

  it should "fail with CONNECTION_FAILED without leaking the password" in {
    new WithSettings() {
      val e = intercept[CatalogException](service.listSchemas("broken"))
      e.code shouldBe CatalogErrorCode.ConnectionFailed
      e.getMessage should not include DuckDbCatalogFixture.BrokenPassword
    }
  }

  "listTables" should "list tables and views of exactly the given schema" in {
    new WithSettings() {
      val tables = service.listTables("duck", "sales")
      tables.map(t => t.name -> t.kind) should contain theSameElementsAs List(
        "orders"          -> "TABLE",
        "big_orders"      -> "VIEW",
        "we'ird.\"name\"" -> "TABLE"
      )
      tables.find(_.name == "orders").get.remarks shouldBe Some("All orders")
    }
  }

  it should "not treat the schema name as a LIKE pattern" in {
    new WithSettings() {
      service.listTables("duck", "s_les").map(_.name) shouldBe List("other")
    }
  }

  "listColumns" should "return columns in ordinal order with type, nullability, precision and scale" in {
    new WithSettings() {
      val cols = service.listColumns("duck", "sales", "orders")
      cols.map(_.name) shouldBe List("id", "amount", "created", "note")
      val id = cols.head
      id.nullable shouldBe false
      id.`type`.toUpperCase shouldBe "INTEGER"
      val amount = cols(1)
      amount.nullable shouldBe true
      amount.precision shouldBe Some(10)
      amount.scale shouldBe Some(2)
      cols(2).`type`.toUpperCase shouldBe "DATE"
      cols(2).precision shouldBe None
    }
  }

  "preview" should "return the first rows with column names and types" in {
    new WithSettings() {
      val p = service.preview("duck", "sales", "orders", 2)
      p.columns.map(_.name) shouldBe List("id", "amount", "created", "note")
      p.rows should have size 2
      p.truncated shouldBe true
      p.rows.head shouldBe List(1L, "12.50", "2024-01-31", "first")
      p.rows(1)(3) shouldBe (null: Any)
    }
  }

  it should "not be truncated when every row fits" in {
    new WithSettings() {
      val p = service.preview("duck", "sales", "orders", 10)
      p.rows should have size 3
      p.truncated shouldBe false
    }
  }

  it should "preview views" in {
    new WithSettings() {
      service.preview("duck", "sales", "big_orders", 10).rows.map(_.head) shouldBe List(2L)
    }
  }

  it should "send integers beyond 2^53 as strings" in {
    new WithSettings() {
      val (path, conns) = DuckDbCatalogFixture.create()
      scala.util.Using.resource(java.sql.DriverManager.getConnection(s"jdbc:duckdb:$path")) { c =>
        scala.util.Using.resource(c.createStatement()) { st =>
          st.execute("CREATE TABLE sales.big (a BIGINT, h HUGEINT, s BIGINT)")
          st.execute(
            "INSERT INTO sales.big VALUES (9007199254740993, 9007199254740993, 9007199254740991)"
          )
          st.execute("INSERT INTO sales.big VALUES (-9007199254740993, 5, -9007199254740991)")
        }
      }
      val rows = new CatalogService(conns).preview("duck", "sales", "big", 10).rows
      rows shouldBe List(
        List("9007199254740993", "9007199254740993", 9007199254740991L),
        List("-9007199254740993", 5L, -9007199254740991L)
      )
    }
  }

  it should "quote identifiers containing quotes and dots" in {
    new WithSettings() {
      service.preview("duck", "sales", "we'ird.\"name\"", 10).rows shouldBe List(List(42L))
    }
  }

  it should "clamp the limit between 1 and 1000" in {
    new WithSettings() {
      service.preview("duck", "sales", "orders", 0).rows should have size 1
      service.preview("duck", "sales", "orders", 5000).rows should have size 3
    }
  }

  it should "fail with QUERY_FAILED for a missing table" in {
    new WithSettings() {
      intercept[CatalogException](service.preview("duck", "sales", "nope", 10)).code shouldBe
      CatalogErrorCode.QueryFailed
    }
  }
}
