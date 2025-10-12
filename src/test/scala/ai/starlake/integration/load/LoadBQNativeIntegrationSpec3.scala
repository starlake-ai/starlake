package ai.starlake.integration.load

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main

class LoadBQNativeIntegrationSpec3 extends BigQueryIntegrationSpecBase {
  override def sampleDataDir = theSampleFolder / "sample-data3"
  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "Import / Load / Transform BQ NATIVE" should "succeed" in {
      withEnvs(
        "SL_ROOT" -> theSampleFolder.pathAsString,
        "SL_ENV"  -> "BQ-NATIVE"
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
