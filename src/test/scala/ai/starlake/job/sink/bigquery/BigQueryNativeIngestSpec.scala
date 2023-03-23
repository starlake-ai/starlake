package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import com.google.cloud.bigquery.{BigQueryOptions, TableId}
import org.scalatest.BeforeAndAfterAll

class BigQueryNativeIngestSpec extends TestHelper with BeforeAndAfterAll {
  val bigquery = BigQueryOptions.newBuilder().build().getService()
  override def beforeAll(): Unit = {
    if (sys.env.getOrElse("COMET_GCP_TEST", "false").toBoolean) {
      bigquery.delete(TableId.of("bqtest", "account"))
      bigquery.delete(TableId.of("bqtest", "jobresult"))
    }
  }
  override def afterAll(): Unit = {
    if (sys.env.getOrElse("COMET_GCP_TEST", "false").toBoolean) {
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "account"))
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "jobresult"))
    }
  }

  "Ingest to BigQuery" should "be ingested and stored in a BigQuery table" in {
    if (sys.env.getOrElse("COMET_GCP_TEST", "false").toBoolean) {
      new WithSettings() {
        new SpecTrait(
          domainOrJobFilename = "bqtest.comet.yml",
          sourceDomainOrJobPathname = "/sample/native/bqtest.comet.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/native/XPOSTBL"
        ) {
          cleanMetadata
          cleanDatasets

          logger.info(settings.comet.datasets)
          loadPending
        }
      }
      val tableFound =
        Option(bigquery.getTable(TableId.of("bqtest", "account"))).isDefined
      tableFound should be(true)

    }
  }
}
