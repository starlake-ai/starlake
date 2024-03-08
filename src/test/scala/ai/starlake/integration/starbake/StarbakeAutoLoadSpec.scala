package ai.starlake.integration.starbake

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class StarbakeAutoLoadSpec extends IntegrationTestBase {

  logger.info(starlakeDir.pathAsString)

  override protected def cleanup(): Unit = {
    // do not cleanup between tests
  }

  override def localDir = starlakeDir / "samples" / "starbake"
  override def sampleDataDir = localDir / "sample-data"
  logger.info(localDir.pathAsString)
  "Autoload" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      copyFilesToIncomingDir(sampleDataDir)
      Main.main(Array("autoload", "--clean"))
    }
  }
}
