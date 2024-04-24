package ai.starlake.integration.utils

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class ValidateSpec extends IntegrationTestBase {

  "Validating Domain" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString) {
      assert(
        new Main().run(
          Array("validate")
        )
      )
    }
  }
}
