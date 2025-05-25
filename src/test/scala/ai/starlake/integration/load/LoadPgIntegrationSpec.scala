package ai.starlake.integration.load

import ai.starlake.integration.JDBCIntegrationSpecBase
import ai.starlake.job.Main

class LoadPgIntegrationSpec extends JDBCIntegrationSpecBase {
  if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
    "Import / Load / Transform PG" should "succeed" in {
      pending
      withEnvs(
        "SL_ROOT" -> theSampleFolder.pathAsString,
        "SL_ENV"  -> "PG"
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
    "Import / Load / Transform PG 2" should "succeed" in {
      pending
      withEnvs(
        "SL_ROOT" -> theSampleFolder.pathAsString,
        "SL_ENV"  -> "PG"
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
