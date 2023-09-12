package ai.starlake.integration

import ai.starlake.job.Main

class LoadLocalIntegrationSpec extends IntegrationSpecBase {
  "Import / Load / Transform Local" should "succeed" in {
    setEnv("SL_ENV", "LOCAL")
    setEnv("SL_INTERNAL_SUBSTITUTE_VARS", "true")
    setEnv("SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE", "dynamic")
    setEnv("SL_MERGE_OPTIMIZE_PARTITION_WRITE", "true")
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
