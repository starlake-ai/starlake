package ai.starlake.integration.load

import ai.starlake.integration.JDBCIntegrationSpecBase
import ai.starlake.job.Main

class LoadRedshiftIntegrationSpec extends JDBCIntegrationSpecBase {
  if (
    false && sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean && sys.env.contains(
      "REDSHIFT_USER"
    )
  ) {
    "Import / Load / Transform Redshift" should "succeed" in {
      withEnvs(
        "SL_ROOT" -> theSampleFolder.pathAsString,
        "SL_ENV"  -> "REDSHIFT_SPARK"
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
      }
    }
  }
}
