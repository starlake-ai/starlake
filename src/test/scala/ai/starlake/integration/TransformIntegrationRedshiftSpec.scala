package ai.starlake.integration

import ai.starlake.job.Main

class TransformIntegrationRedshiftSpec extends BigQueryIntegrationSpecBase {
  override def templates = starlakeDir / "samples"
  override def localDir = templates / "spark"
  override def sampleDataDir = localDir / "sample-data"

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {

    "Native Snowflake Transform" should "succeed" in {
      withEnvs(
        "SL_ENV"  -> "REDSHIFT",
        "SL_ROOT" -> localDir.pathAsString
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
