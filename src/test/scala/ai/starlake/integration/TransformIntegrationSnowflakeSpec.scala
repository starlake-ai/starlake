package ai.starlake.integration

import ai.starlake.job.Main

class TransformIntegrationSnowflakeSpec extends BigQueryIntegrationSpecBase {
  val snowflakeDir = starlakeDir / "samples" / "spark"

  if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {

    "Native Snowflake Transform" should "succeed" in {
      withEnvs(
        "SL_ENV"  -> "SNOWFLAKE",
        "SL_ROOT" -> snowflakeDir.pathAsString
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
