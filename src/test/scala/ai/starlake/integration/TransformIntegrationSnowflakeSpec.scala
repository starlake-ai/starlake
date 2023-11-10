package ai.starlake.integration

import ai.starlake.job.Main

class TransformIntegrationSnowflakeSpec extends BigQueryIntegrationSpecBase {
  val snowflakeDir = starlakeDir / "samples" / "snowflake"

  if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {

    "Native Snowflake Transform" should "succeed" in {
      withEnvs(
        "SL_ROOT" -> snowflakeDir.pathAsString
      ) {
        clearDataDirectories()
        incomingDir.copyToDirectory(localDir)
        Main.main(
          Array("transform", "--name", "kpi.byseller")
        )
      }
    }
  }
}
