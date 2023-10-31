package ai.starlake.integration

import ai.starlake.job.Main

class TransformIntegrationSpec extends BigQueryIntegrationSpecBase {
  if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {

    "Import / Load / Transform BQ" should "succeed" in {
      withEnvs(
        "SL_ROOT"                                       -> localDir.pathAsString,
        "SL_ENV"                                        -> "BQ",
        "SL_INTERNAL_SUBSTITUTE_VARS"                   -> "true",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "dynamic",
        "SL_MERGE_OPTIMIZE_PARTITION_WRITE"             -> "true"
      ) {
        clearDataDirectories()
        incomingDir.copyToDirectory(localDir)
        Main.main(
          Array("transform", "--name", "sales_kpi.byseller_kpi", "--compile")
        )
      }
    }
  }
}
