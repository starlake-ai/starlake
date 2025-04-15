package ai.starlake.integration.starbake

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main

class DagGenerateStarbakeSpec extends BigQueryIntegrationSpecBase {
  logger.info(starlakeDir.pathAsString)
  override val theSampleFolder = samplesFolder / "starbake"

  override protected def cleanup(): Unit = {
    // do not cleanup between tests
  }

  "All Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> theSampleFolder.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "all"
    ) {
      copyFilesToIncomingDir(sampleDataDir)

      assert(
        new Main().run(
          Array("dag-generate", "--clean", "--tags", "cust")
        )
      )
    }
  }
}
