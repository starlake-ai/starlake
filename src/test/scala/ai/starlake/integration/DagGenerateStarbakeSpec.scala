package ai.starlake.integration

import ai.starlake.job.Main

class DagGenerateStarbakeSpec extends BigQueryIntegrationSpecBase {
  val samplesDir = starlakeDir / "samples"
  logger.info(starlakeDir.pathAsString)
  override val localDir = samplesDir / "starbake"

  "All Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> localDir.pathAsString,
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
