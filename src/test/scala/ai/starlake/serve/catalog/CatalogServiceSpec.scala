package ai.starlake.serve.catalog

import ai.starlake.TestHelper

class CatalogServiceSpec extends TestHelper {

  private lazy val (_, connections) = DuckDbCatalogFixture.create()

  private def service(implicit settings: ai.starlake.config.Settings) =
    new CatalogService(connections)

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
}
