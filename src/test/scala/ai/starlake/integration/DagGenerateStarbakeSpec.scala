package ai.starlake.integration

import ai.starlake.job.Main

class DagGenerateStarbakeSpec extends BigQueryIntegrationSpecBase {
  val samplesDir = starlakeDir / "samples"
  logger.info(starlakeDir.pathAsString)
  val starbakeDir = samplesDir / "starbake"
  logger.info(starbakeDir.pathAsString)

  "All Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> starbakeDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "all"
    ) {
      clearDataDirectories()
      sampleDataDir.copyToDirectory(localDir)

      Main.main(
        Array("dag-generate", "--clean", "--tags", "cust")
      )
    }
  }
}
