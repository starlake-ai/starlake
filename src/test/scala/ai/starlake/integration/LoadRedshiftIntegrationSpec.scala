package ai.starlake.integration

import ai.starlake.job.Main

class LoadRedshiftIntegrationSpec extends BigQueryIntegrationSpecBase {
  override def templates = starlakeDir / "samples"
  override def localDir = templates / "spark"
  override def sampleDataDir = localDir / "sample-data"

  "Import / Load / Transform BQ" should "succeed" in {
    withEnvs(
      "SL_ROOT"                                       -> localDir.pathAsString,
      "SL_ENV"                                        -> "REDSHIFT",
      "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC"
    ) {
      clearDataDirectories()
      Main.main(
        Array("import")
      )
      Main.main(
        Array("load")
      )
    }
  }
}
