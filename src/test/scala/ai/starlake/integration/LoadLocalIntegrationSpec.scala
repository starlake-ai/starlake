package ai.starlake.integration

import ai.starlake.job.Main

class LoadLocalIntegrationSpec extends BigQueryIntegrationSpecBase {
  "Import / Load / Transform Local" should "succeed" in {
    withEnvs(
      "SL_ROOT"                                       -> localDir.pathAsString,
      "SL_INTERNAL_SUBSTITUTE_VARS"                   -> "true",
      "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
      "SL_MERGE_OPTIMIZE_PARTITION_WRITE"             -> "true"
    ) {
      clearDataDirectories()
      sampleDataDir.copyToDirectory(localDir)
      Main.main(
        Array("import")
      )
      Main.main(
        Array("load")
      )
    }
  }
}
