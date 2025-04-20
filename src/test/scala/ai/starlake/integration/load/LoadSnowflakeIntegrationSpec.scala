package ai.starlake.integration.load

import ai.starlake.integration.JDBCIntegrationSpecBase
import ai.starlake.job.Main

class LoadSnowflakeIntegrationSpec extends JDBCIntegrationSpecBase {
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
              Array("import")
            )
          )
          assert(
            new Main().run(
              Array("load")
            )
          )
        }
      }
      "Import / Load / Transform Snowflake 2" should "succeed" in {
        withEnvs(
          "SL_ROOT" -> theSampleFolder.pathAsString,
          "SL_ENV"  -> "SNOWFLAKE"
        ) {
          val sampleDataDir2 = theSampleFolder / "sample-data2"
          copyFilesToIncomingDir(sampleDataDir2)
          assert(
            new Main().run(
              Array("import")
            )
          )
          assert(
            new Main().run(
              Array("load")
            )
          )
        }
      }
    }
  }
}
