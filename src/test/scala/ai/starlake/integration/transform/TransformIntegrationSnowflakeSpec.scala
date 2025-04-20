package ai.starlake.integration.transform

import ai.starlake.config.DatasetArea
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class TransformIntegrationSnowflakeSpec extends IntegrationTestBase {
  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "Native Snowflake Transform" should "succeed" in {
      withEnvs(
        "SL_ENV"  -> "SNOWFLAKE",
        "SL_ROOT" -> theSampleFolder.pathAsString
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
    "Native Snowflake Extract Table Schema" should "succeed" in {
      withEnvs(
        "SL_ENV"  -> "SNOWFLAKE",
        "SL_ROOT" -> theSampleFolder.pathAsString
      ) {
        cleanup()
        assert(
          new Main().run(
            Array(
              "extract-schema",
              "--external",
              "--tables",
              "audit.audit",
              "--outputDir",
              DatasetArea.external.toString
            )
          )
        )
      }
    }
  }
}
