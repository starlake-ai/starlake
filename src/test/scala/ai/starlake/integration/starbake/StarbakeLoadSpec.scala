package ai.starlake.integration.starbake

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class StarbakeLoadSpec extends IntegrationTestBase {

  val samplesDir = starlakeDir / "samples"
  logger.info(starlakeDir.pathAsString)
  val starbakeDir = samplesDir / "starbake2"
  logger.info(starbakeDir.pathAsString)

  "Infer Schema" should "succeed" in {
    withEnvs("SL_ROOT" -> starbakeDir.pathAsString) {
      Main.main(
        Array(
          "infer-schema",
          "--input",
          s"$starbakeDir/incoming/starbake",
          "--clean"
        )
      )
    }
  }

}
