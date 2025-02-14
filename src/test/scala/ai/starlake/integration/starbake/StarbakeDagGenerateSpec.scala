package ai.starlake.integration.starbake

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class StarbakeDagGenerateSpec extends IntegrationTestBase {

  logger.info(starlakeDir.pathAsString)

  override protected def cleanup(): Unit = {
    // do not cleanup between tests
  }

  override def localDir = starlakeDir / "samples" / "starbake"
  override def sampleDataDir = localDir / "sample-data"
  logger.info(localDir.pathAsString)
  "Dag Generate" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      copyFilesToIncomingDir(sampleDataDir)
      assert(new Main().run(Array("dag-generate", "--clean", "--orchestrator", "snowflake")))
    }
  }
}
