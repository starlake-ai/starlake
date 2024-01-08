package ai.starlake.integration

import ai.starlake.job.Main

class ValidateSpec extends IntegrationTestBase {

  "Validating Domain" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString) {
      Main.main(
        Array("validate")
      )
    }
  }
}
