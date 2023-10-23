package ai.starlake.integration

import ai.starlake.job.Main
import better.files.File

class ValidateSpec extends IntegrationTestBase {

  val starlakeDir = File(".")
  logger.info(starlakeDir.pathAsString)
  val localDir = starlakeDir / "samples" / "local"
  val incomingDir = localDir / "incoming"
  val quickstartDir: File = localDir / "quickstart"
  val directoriesToClear = List("incoming", "audit", "datasets", "diagrams")

  "Validating Domain" should "succeed" in {
    withEnvs("SL_ROOT" -> quickstartDir.pathAsString) {
      Main.main(
        Array("validate")
      )
    }
  }
}
