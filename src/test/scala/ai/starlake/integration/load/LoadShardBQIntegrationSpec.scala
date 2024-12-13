package ai.starlake.integration.load

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main
import better.files.File

class LoadShardBQIntegrationSpec extends BigQueryIntegrationSpecBase {
  override def templates: File = starlakeDir / "samples"
  override def localDir: File = templates / "spark"
  override def sampleDataDir: File = localDir / "sample-shard"
  "Import / Load / Transform BQ Shard" should "succeed" in {
    withEnvs(
      "SL_ROOT" -> localDir.pathAsString,
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
}
