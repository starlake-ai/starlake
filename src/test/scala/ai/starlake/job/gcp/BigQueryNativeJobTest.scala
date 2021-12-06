package ai.starlake.job.gcp

import ai.starlake.TestHelper

class BigQueryNativeJobTest extends TestHelper {
  "Ingest to BigQuery" should "should be ingested and store table in BigQuery" in {
    if (sys.env.getOrElse("COMET_GCP_TEST", "false").toBoolean) {
      import org.slf4j.impl.StaticLoggerBinder
      val binder = StaticLoggerBinder.getSingleton
      logger.debug(binder.getLoggerFactory.toString)
      logger.debug(binder.getLoggerFactoryClassStr)

      new WithSettings() {
        new SpecTrait(
          domainOrJobFilename = "bqtest.comet.yml",
          sourceDomainOrJobPathname = "/sample/position/bqtest.comet.yml",
          datasetDomainName = "bqtest",
          sourceDatasetPathName = "/sample/position/XPOSTBL"
        ) {
          cleanMetadata
          cleanDatasets

          logger.info(settings.comet.datasets)
          loadPending
        }
      }
    }
  }
}
