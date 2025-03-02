package ai.starlake.integration.starbake

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File

class StarbakeAutoLoadSpec extends IntegrationTestBase {

  logger.info(starlakeDir.pathAsString)

  override protected def cleanup(): Unit = {
    // do not cleanup between tests
  }

  override def localDir: File = starlakeDir / "samples" / "starbake"
  override def sampleDataDir: File = localDir / "sample-data"
  override def incomingDir: File = localDir / "datasets" / "incoming"
  logger.info(localDir.pathAsString)
  "Autoload" should "succeed" in {
    withEnvs(
      "SL_ROOT" -> localDir.pathAsString,
      "SL_ENV"  -> "DUCKDB"
    ) {
      copyFilesToIncomingDir(sampleDataDir)
      assert(new Main().run(Array("autoload", "--clean")))
    }
  }
}
