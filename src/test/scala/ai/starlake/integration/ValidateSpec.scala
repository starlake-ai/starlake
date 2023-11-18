package ai.starlake.integration

import ai.starlake.job.Main

class ValidateSpec extends IntegrationTestBase {

  val directoriesToClear = List("incoming", "audit", "datasets", "diagrams")

  "Validating Domain" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString) {
      Main.main(
        Array("validate")
      )
    }
  }
}
