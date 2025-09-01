package ai.starlake.integration.freshness

import ai.starlake.integration.JDBCIntegrationSpecBase
import ai.starlake.job.Main

class FreshnessSnowflakeIntegrationSpec extends JDBCIntegrationSpecBase {

  override def sampleDataDir = theSampleFolder / "sample-data"
  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    if (sys.env.contains("SNOWFLAKE_ACCOUNT")) {
      "Import / Load / Transform Snowflake" should "succeed" in {
        withEnvs(
          "SL_ROOT" -> theSampleFolder.pathAsString,
          "SL_ENV"  -> "SNOWFLAKE"
        ) {
          cleanup()
          copyFilesToIncomingDir(sampleDataDir)
          assert(
            new Main().run(
              Array("stage")
            )
          )
          assert(
            new Main().run(
              Array("load")
            )
          )
          assert(
            new Main().run(
              Array("freshness", "--tables", "sales.customers,sales.orders,hr.sellers")
            )
          )
        }
      }
    }
  }
}
