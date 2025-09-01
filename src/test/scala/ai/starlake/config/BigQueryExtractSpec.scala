package ai.starlake.config

import ai.starlake.TestHelper
import ai.starlake.extract.{ExtractBigQuerySchema, TablesExtractConfig}
import com.google.cloud.bigquery.{BigQueryOptions, TableId}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll

class BigQueryExtractSpec extends TestHelper with BeforeAndAfterAll {
  val bigquery = BigQueryOptions.newBuilder().build().getService()
  override def beforeAll(): Unit = {
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      bigquery.delete(TableId.of("bqtest", "account"))
      bigquery.delete(TableId.of("bqtest", "jobresult"))
    }
  }
  override def afterAll(): Unit = {
    super.afterAll()
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "account"))
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "jobresult"))
    }
  }
  " BigQuery Extract" should "succeed" in {
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      val bigQueryConfiguration: Config = {
        val config = ConfigFactory.parseString("""
            |connections.spark {
            |  sparkFormat = "bigquery"
            |  type = "bigquery"
            |  options {
            |    gcsBucket: starlake-app
            |    location: "europe-west1"
            |    authType: APPLICATION_DEFAULT
            |    #authType: SERVICE_ACCOUNT_JSON_KEYFILE
            |    #jsonKeyfile: "/Users/me/.gcloud/keys/my-key.json"
            |  }
            |}
            |""".stripMargin)
        val result = config.withFallback(super.testConfiguration)
        result
      }
      new WithSettings(bigQueryConfiguration) {
        new SpecTrait(
          sourceDomainOrJobPathname = "/sample/position/bqtest.sl.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/position/XPOSTBL"
        ) {
          val schemaHandler = settings.schemaHandler()
          val domains = new ExtractBigQuerySchema(TablesExtractConfig(None, None))
            .extractSchemasAndTables(schemaHandler, Map.empty)
          println(domains.size)
          domains.foreach(println)
        }
      }
    }
  }
}
