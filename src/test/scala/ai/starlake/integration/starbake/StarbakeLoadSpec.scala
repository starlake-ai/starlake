package ai.starlake.integration.starbake

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class StarbakeLoadSpec extends IntegrationTestBase {

  logger.info(starlakeDir.pathAsString)

  override protected def cleanup(): Unit = {
    // do not cleanup between tests
  }
  override def incomingDir = theSampleFolder / "datasets" / "incoming"
  override def theSampleFolder = starlakeDir / "samples" / "starbake"
  override def sampleDataDir = theSampleFolder / "sample-data"
  logger.info(theSampleFolder.pathAsString)
  "Autoload" should "succeed" in {
    withEnvs("SL_ROOT" -> theSampleFolder.pathAsString, "SL_ENV" -> "DUCKDB") {
      copyFilesToIncomingDir(sampleDataDir)
      assert(new Main().run(Array("stage")))
      assert(new Main().run(Array("load")))
    }
  }
}
