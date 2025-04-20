package ai.starlake.integration.load

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main
import better.files.File

class LoadBQIntegrationSpec extends BigQueryIntegrationSpecBase {
  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "Import / Load / Transform BQ" should "succeed" in {
      withEnvs(
        "SL_ROOT" -> theSampleFolder.pathAsString,
        "SL_ENV"  -> "BQ"
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        assert(
          new Main().run(
            Array("import")
          )
        )
        assert(new Main().run(Array("load")))
      }
    }
    "Import / Load / Transform BQ 2" should "succeed" in {
      withEnvs(
        "SL_ROOT" -> theSampleFolder.pathAsString,
        "SL_ENV"  -> "BQ"
      ) {
        val sampleDataDir2 = theSampleFolder / "sample-data2"
        sampleDataDir2.copyTo(incomingDir)

        assert(
          new Main().run(
            Array("import")
          )
        )
        assert(new Main().run(Array("load")))

      }
    }
  }
}
