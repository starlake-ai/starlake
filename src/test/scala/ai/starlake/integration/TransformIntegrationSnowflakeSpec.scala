package ai.starlake.integration

import ai.starlake.job.Main

class TransformIntegrationSnowflakeSpec extends IntegrationTestBase {
  val snowflakeDir = starlakeDir / "samples" / "spark"

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "Native Snowflake Transform" should "succeed" in {
      withEnvs(
        "SL_ENV"  -> "SNOWFLAKE",
        "SL_ROOT" -> snowflakeDir.pathAsString
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        assert(
          new Main().run(
            Array("transform", "--name", "sales_kpi.byseller_kpi0")
          )
        )
      }
    }
  }
}
