package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.job.ingest.PositionIngestionJob
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

class SnowflakeNativePositionSpec extends TestHelper {

  lazy val snowflakeConfiguration: Config = {
    val config = ConfigFactory.parseString(
      """
        |connectionRef: "test-snowflake"
        |connections.test-snowflake {
        |    type = "jdbc"
        |    options {
        |      "url": "jdbc:snowflake://test.snowflakecomputing.com/?db=TESTDB"
        |      "driver": "net.snowflake.client.jdbc.SnowflakeDriver"
        |      "user": "fake"
        |      "password": "fake"
        |    }
        |}
        |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(snowflakeConfiguration) {

    def positionJob(): PositionIngestionJob = {
      val schemaHandler = settings.schemaHandler(reload = true)
      val domain = schemaHandler
        .domains()
        .find(_.name == "positionsnow")
        .getOrElse(fail("positionsnow domain not found"))
      val table = domain.tables
        .find(_.name == "account")
        .getOrElse(fail("account table not found"))
      new PositionIngestionJob(
        domain,
        table,
        schemaHandler.types(),
        List(new Path("/incoming/XPOSSNOWTBL")),
        settings.storageHandler(),
        schemaHandler,
        Map.empty,
        None,
        false,
        None
      )
    }

    "Snowflake statement generation for a POSITION table" should "slice lines with SUBSTR wrapped in TRY_CAST" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/positionsnow/positionsnow.sl.yml",
        datasetDomainName = "positionsnow",
        sourceDatasetPathName = "/sample/positionsnow/XPOSSNOWTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "positionsnow",
          "/sample/positionsnow/account_positionsnow.sl.yml",
          Some("account.sl.yml")
        )

        val statements = positionJob().buildListOfSQLStatementsAsMap("snowflake")

        val loadTaskSQL =
          statements("statements").asInstanceOf[java.util.Map[String, Object]]

        val secondStep = loadTaskSQL.get("secondStep").toString.replaceAll("\\s+", " ")
        secondStep should include("SUBSTR(value, 1, 10)")
        secondStep should include("TRY_CAST(SUBSTR(value, 11, 5) AS INTEGER)")
        secondStep should not include "SAFE_CAST"

        // the first-step temp table holds the raw line as a single VARCHAR column
        val firstStep = loadTaskSQL.get("firstStep").toString.replaceAll("\\s+", " ")
        firstStep should include("value")
        firstStep should not include "amount"
      }
    }

    "Snowflake COPY for a POSITION file" should "load whole lines with FIELD_DELIMITER = NONE" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/positionsnow/positionsnow.sl.yml",
        datasetDomainName = "positionsnow",
        sourceDatasetPathName = "/sample/positionsnow/XPOSSNOWTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "positionsnow",
          "/sample/positionsnow/account_positionsnow.sl.yml",
          Some("account.sl.yml")
        )

        val loader = new SnowflakeNativeLoader(positionJob())
        val sql = loader.buildCopyPosition("positionsnow.zztmp_account").replaceAll("\\s+", " ")

        sql should include("COPY INTO positionsnow.zztmp_account")
        sql should include("TYPE = CSV")
        sql should include("FIELD_DELIMITER = NONE")
        sql should include("FIELD_OPTIONALLY_ENCLOSED_BY = NONE")
        sql should include("ESCAPE_UNENCLOSED_FIELD = NONE")
        // withHeader is false in the domain metadata
        sql should include("SKIP_HEADER = 0")
        sql should include("ENCODING = 'UTF-8'")
      }
    }
  }
}
