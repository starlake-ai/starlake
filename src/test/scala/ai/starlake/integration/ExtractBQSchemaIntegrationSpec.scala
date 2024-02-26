package ai.starlake.integration

import ai.starlake.job.Main

class ExtractBQSchemaIntegrationSpec extends BigQueryIntegrationSpecBase {
  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {

    "External Load" should "succeed" in {
      withEnvs(
        "SL_ROOT"                                       -> localDir.pathAsString,
        "SL_ENV"                                        -> "BQ-NATIVE",
        "SL_INTERNAL_SUBSTITUTE_VARS"                   -> "true",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
        "SL_MERGE_OPTIMIZE_PARTITION_WRITE"             -> "true"
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        Main.main(
          Array("extract-bq-schema", "--external")
        )
      }
    }
  }
}
