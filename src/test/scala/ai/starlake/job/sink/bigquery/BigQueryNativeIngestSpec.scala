package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import com.google.cloud.bigquery.{BigQueryOptions, TableId}
import org.scalatest.BeforeAndAfterAll

class BigQueryNativeIngestSpec extends TestHelper with BeforeAndAfterAll {
  val bigquery = BigQueryOptions.newBuilder().build().getService()
  override def beforeAll(): Unit = {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      bigquery.delete(TableId.of("nativesales", "nativecustomers"))
    }
  }
  override def afterAll(): Unit = {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "account"))
      // BigQueryJobBase.bigquery.delete(TableId.of("bqtest", "jobresult"))
    }
  }

  "Ingest to BigQuery" should "be ingested and stored in a BigQuery table using native mode" in {
    if (false && sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      import org.slf4j.impl.StaticLoggerBinder
      val binder = StaticLoggerBinder.getSingleton
      logger.debug(binder.getLoggerFactory.toString)
      logger.debug(binder.getLoggerFactoryClassStr)

      new WithSettings() {
        new SpecTrait(
          sourceDomainOrJobPathname = "/sample/native/nativesales.sl.yml",
          datasetDomainName = "nativesales",
          sourceDatasetPathName = "/sample/native/nativecustomers.psv"
        ) {
          cleanMetadata
          cleanDatasets

          logger.info(settings.appConfig.datasets)
          loadPending
        }
      }
      val tableFound =
        Option(bigquery.getTable(TableId.of("nativesales", "nativecustomers"))).isDefined
      tableFound should be(true)

    }
  }
}
