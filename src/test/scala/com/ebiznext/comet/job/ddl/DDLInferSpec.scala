package com.ebiznext.comet.job.ddl

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.schema.generator.{Yml2DDLConfig, Yml2DDLJob}
import com.ebiznext.comet.schema.handlers.SchemaHandler
import com.ebiznext.comet.utils.Utils

class DDLInferSpec extends TestHelper {

  "Infer Create BQ Table" should "succeed" in {
    import org.slf4j.impl.StaticLoggerBinder
    val binder = StaticLoggerBinder.getSingleton
    logger.debug(binder.getLoggerFactory.toString)
    logger.debug(binder.getLoggerFactoryClassStr)

    new WithSettings() {
      new SpecTrait(
        domainOrJobFilename = "position.comet.yml",
        sourceDomainOrJobPathname = "/sample/position/position.comet.yml",
        datasetDomainName = "position",
        sourceDatasetPathName = "/sample/position/XPOSTBL"
      ) {
        val schemaHandler = new SchemaHandler(metadataStorageHandler)
        cleanMetadata
        cleanDatasets
        val config = Yml2DDLConfig("bigquery")
        val result = new Yml2DDLJob(config, schemaHandler).run()
        Utils.logFailure(result, logger)
      }

    }
  }
}
