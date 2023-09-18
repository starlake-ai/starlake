package ai.starlake.integration

import ai.starlake.job.Main

class LoadBQNativeIntegrationSpec extends BigQueryIntegrationSpecBase {
  if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {

    "Import / Load / Transform BQ NATIVE" should "succeed" in {

      setEnv("SL_ENV", "BQ-NATIVE")
      setEnv("SL_INTERNAL_SUBSTITUTE_VARS", "true")
      setEnv("SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE", "dynamic")
      setEnv("SL_MERGE_OPTIMIZE_PARTITION_WRITE", "true")
      setEnv("SL_ISSUE_SPARK_BIGQUERY_1060", "")
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
