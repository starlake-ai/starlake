package ai.starlake.config

import ai.starlake.TestHelper
import ai.starlake.extract.{ExtractBigQuerySchema, TablesExtractConfig}
import com.typesafe.config.{Config, ConfigFactory}

// This suite only reads from BigQuery (schema extraction): it must not delete
// tables in shared datasets, and its local domain uses a per-run dataset name.
class BigQueryExtractSpec extends TestHelper {
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
          datasetDomainName = testBQDatasetName,
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
