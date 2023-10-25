package ai.starlake.integration

import ai.starlake.job.Main

class LocalBQIntegrationSpec extends BigQueryIntegrationSpecBase {
  if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
    "Import / Load / Transform BQ" should "succeed" in {
      withEnvs(
        "SL_ROOT"                                       -> quickstartDir.pathAsString,
        "SL_ENV"                                        -> "BQ",
        "SL_INTERNAL_SUBSTITUTE_VARS"                   -> "true",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "dynamic",
        "SL_MERGE_OPTIMIZE_PARTITION_WRITE"             -> "true"
      ) {
        clearDataDirectories()
        incomingDir.copyToDirectory(quickstartDir)
        Main.main(
          Array("import")
        )
        Main.main(
          Array("load")
        )
      }
    }
  }
}
