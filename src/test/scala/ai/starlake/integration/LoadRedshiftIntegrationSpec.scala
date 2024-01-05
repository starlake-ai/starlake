package ai.starlake.integration

import ai.starlake.job.Main

class LoadRedshiftIntegrationSpec extends JDBCIntegrationSpecBase {
  override def templates = starlakeDir / "samples"
  override def localDir = templates / "spark"
  override def sampleDataDir = localDir / "sample-data"

  "Import / Load / Transform Redshift" should "succeed" in {
    withEnvs(
      "SL_ROOT"                                       -> localDir.pathAsString,
      "SL_ENV"                                        -> "REDSHIFT",
      "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC"
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
  "Import / Load / Transform Redshift 2" should "succeed" in {
    withEnvs(
      "SL_ROOT"                                       -> localDir.pathAsString,
      "SL_ENV"                                        -> "PG",
      "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC"
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
