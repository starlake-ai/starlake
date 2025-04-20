package ai.starlake.integration.extract

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main

class ExtractBQSchemaIntegrationSpec extends BigQueryIntegrationSpecBase {
  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {

    "External Load" should "succeed" in {
      withEnvs(
        "SL_ROOT"                                       -> theSampleFolder.pathAsString,
        "SL_ENV"                                        -> "BQ-NATIVE",
        "SL_INTERNAL_SUBSTITUTE_VARS"                   -> "true",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
        "SL_MERGE_OPTIMIZE_PARTITION_WRITE"             -> "true"
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        assert(
          new Main().run(
            Array("extract-bq-schema", "--external")
          )
        )
      }
    }
  }
}
