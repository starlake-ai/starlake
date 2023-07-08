package ai.starlake.config

import ai.starlake.TestHelper
import ai.starlake.extract.BigQueryTablesConfig
import ai.starlake.schema.generator.ExtractBigQuerySchema
import com.google.cloud.bigquery.{BigQueryOptions, TableId}
import org.scalatest.BeforeAndAfterAll

class BigQueryExtractSpec extends TestHelper with BeforeAndAfterAll {
  val bigquery = BigQueryOptions.newBuilder().build().getService()
  override def beforeAll(): Unit = {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      bigquery.delete(TableId.of("bqtest", "account"))
      bigquery.delete(TableId.of("bqtest", "jobresult"))
    }
  }
  override def afterAll(): Unit = {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "account"))
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "jobresult"))
    }
  }
  " BigQuery Extract" should "succeed" in {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      new WithSettings() {
        new SpecTrait(
          domainOrJobFilename = "bqtest.comet.yml",
          sourceDomainOrJobPathname = "/sample/position/bqtest.comet.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/position/XPOSTBL"
        ) {
          val domains = new ExtractBigQuerySchema(BigQueryTablesConfig(None, None))
            .extractDatasets()
        }
      }
    }
  }
}
