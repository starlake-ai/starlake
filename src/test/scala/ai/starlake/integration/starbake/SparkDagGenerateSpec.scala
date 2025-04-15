package ai.starlake.integration.starbake

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class SparkDagGenerateSpec extends IntegrationTestBase {

  logger.info(starlakeDir.pathAsString)

  override protected def cleanup(): Unit = {
    // do not cleanup between tests
  }

  logger.info(theSampleFolder.pathAsString)
  "Dag Generate" should "succeed" in {
    withEnvs("SL_ROOT" -> theSampleFolder.pathAsString, "SL_ENV" -> "SNOWFLAKE") {
      copyFilesToIncomingDir(sampleDataDir)
      assert(new Main().run(Array("dag-generate", "--clean", "--orchestrator", "snowflake")))
    }
  }
}
