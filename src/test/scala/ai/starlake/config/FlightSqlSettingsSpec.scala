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

    it should "default the arrow flight driver even when the dialect resolves to snowflake" in {
      val conn = ConnectionInfo(
        `type` = ConnectionType.JDBC,
        sparkFormat = Some("jdbc"),
        options = Map("url" -> qodUrl, "dialect" -> "snowflake")
      )
      conn.isSnowflake() shouldBe true // the dialect shadows the flight transport
      val appConfig = settings.appConfig.copy(connections = Map("qod" -> conn))
      val adjusted = Settings.adjustConnectionProperties(settings.copy(appConfig = appConfig))
      val adjustedConn = adjusted.appConfig.connections("qod")
      adjustedConn.options("driver") shouldBe ConnectionInfo.ArrowFlightDriverClass
      adjustedConn.sparkFormat shouldBe Some("jdbc")
      adjustedConn.options("url") shouldBe qodUrl
    }
  }
}
