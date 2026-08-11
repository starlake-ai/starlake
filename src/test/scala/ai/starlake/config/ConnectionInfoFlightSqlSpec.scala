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
