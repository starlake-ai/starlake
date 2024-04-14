package ai.starlake.integration

import ai.starlake.job.Main

class LoadSnowflakeIntegrationSpec extends JDBCIntegrationSpecBase {
  override def templates = starlakeDir / "samples"

  override def localDir = templates / "spark"

  override def sampleDataDir = localDir / "sample-data"
  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {

    if (sys.env.contains("SNOWFLAKE_ACCOUNT")) {
      "Import / Load / Transform Snowflake" should "succeed" in {
        withEnvs(
          "SL_ROOT" -> localDir.pathAsString,
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
          "SL_ROOT" -> localDir.pathAsString,
          "SL_ENV"  -> "SNOWFLAKE"
        ) {
          val sampleDataDir2 = localDir / "sample-data2"
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
