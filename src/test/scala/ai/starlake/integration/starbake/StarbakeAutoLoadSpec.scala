package ai.starlake.integration.starbake

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File

class StarbakeAutoLoadSpec extends IntegrationTestBase {
  logger.info(starlakeDir.pathAsString)

  override protected def cleanup(): Unit = {
    // do not cleanup between tests
  }

  override def theSampleFolder: File = samplesFolder / "starbake"
  override def sampleDataDir: File = theSampleFolder / "sample-data"
  override def incomingDir: File = theSampleFolder / "datasets" / "incoming"
  logger.info(theSampleFolder.pathAsString)
  "Autoload" should "succeed" in {
    withEnvs(
      "SL_ROOT" -> theSampleFolder.pathAsString,
      "SL_ENV"  -> "DUCKDB"
    ) {
      copyFilesToIncomingDir(sampleDataDir)
      assert(new Main().run(Array("autoload", "--clean")))
    }
  }
}
