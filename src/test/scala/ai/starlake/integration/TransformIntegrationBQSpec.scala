package ai.starlake.integration

import ai.starlake.job.Main

class TransformIntegrationBQSpec extends BigQueryIntegrationSpecBase {
  override def beforeAll(): Unit = {}
  override def templates = starlakeDir / "samples"

  override def localDir = templates / "spark"

  override def sampleDataDir = localDir / "sample-data"

  if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {

    "Native Snowflake Transform" should "succeed" in {
      withEnvs(
        "SL_ENV"                                        -> "BQ",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
        "SL_ROOT"                                       -> localDir.pathAsString
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        Main.main(
          Array("transform", "--name", "sales_kpi.byseller_kpi")
        )
      }
    }
  }
}
