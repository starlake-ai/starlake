package ai.starlake.integration

import ai.starlake.job.Main

class LoadSnowflakeIntegrationSpec extends JDBCIntegrationSpecBase {
  override def templates = starlakeDir / "samples"

  override def localDir = templates / "spark"

  override def sampleDataDir = localDir / "sample-data"

  "Import / Load / Transform Snowflake" should "succeed" in {
    withEnvs(
      "SL_ROOT"                                       -> localDir.pathAsString,
      "SL_ENV"                                        -> "SNOWFLAKE",
      "SL_INTERNAL_SUBSTITUTE_VARS"                   -> "true",
      "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
      "SL_MERGE_OPTIMIZE_PARTITION_WRITE"             -> "true"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)

      Main.main(
        Array("import")
      )
      Main.main(
        Array("load")
      )
    }
  }
  "Import / Load / Transform Snowflake 2" should "succeed" in {
    withEnvs(
      "SL_ROOT"                                       -> localDir.pathAsString,
      "SL_ENV"                                        -> "SNOWFLAKE",
      "SL_INTERNAL_SUBSTITUTE_VARS"                   -> "true",
      "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
      "SL_MERGE_OPTIMIZE_PARTITION_WRITE"             -> "true"
    ) {
      val sampleDataDir2 = localDir / "sample-data2"
      copyFilesToIncomingDir(sampleDataDir2)
      Main.main(
        Array("import")
      )
      Main.main(
        Array("load")
      )
    }
  }
}
