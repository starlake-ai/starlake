package ai.starlake.integration.load

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main
import better.files.File

class LoadShardBQIntegrationSpec extends BigQueryIntegrationSpecBase {
  override def sampleDataDir: File = theSampleFolder / "sample-shard"
  "Import / Load / Transform BQ Shard" should "succeed" in {
    withEnvs(
      "SL_ROOT" -> theSampleFolder.pathAsString,
      "SL_ENV"  -> "BQ"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)
      assert(
        new Main().run(
          Array("stage")
        )
      )
      assert(new Main().run(Array("load")))
    }
  }
}
