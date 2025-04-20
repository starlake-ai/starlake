package ai.starlake.integration.transform

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main

class TransformIntegrationSpec extends BigQueryIntegrationSpecBase {
  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {

    "Import / Load / Transform BQ" should "succeed" in {
      withEnvs(
        "SL_ROOT"                                       -> theSampleFolder.pathAsString,
        "SL_ENV"                                        -> "BQ",
        "SL_INTERNAL_SUBSTITUTE_VARS"                   -> "true",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
        "SL_MERGE_OPTIMIZE_PARTITION_WRITE"             -> "true"
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        assert(new Main().run(Array("transform", "--name", "sales_kpi.byseller_kpi0", "--compile")))
      }
    }
  }
}
