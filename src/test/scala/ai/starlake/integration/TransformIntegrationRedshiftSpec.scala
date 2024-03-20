package ai.starlake.integration

import ai.starlake.job.Main

class TransformIntegrationRedshiftSpec extends BigQueryIntegrationSpecBase {
  override def templates = starlakeDir / "samples"
  override def localDir = templates / "spark"
  override def sampleDataDir = localDir / "sample-data"

  if (false && sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {

    "Native REDSHIFT Transform" should "succeed" in {
      withEnvs(
        "SL_ENV"  -> "REDSHIFT",
        "SL_ROOT" -> localDir.pathAsString
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        assert(
          new Main().run(
            Array("transform", "--name", "sales_kpi.byseller_kpi")
          )
        )
      }
    }
  }
}
