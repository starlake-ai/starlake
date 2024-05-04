package ai.starlake.integration.utils

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class BootstrapSpec extends IntegrationTestBase {

  behavior of "BootstrapSpec"
  it should "bootstrap" in {
    if (false) {
      val projectDir = starlakeDir / "bootstrap-test"
      projectDir.createDirectoryIfNotExists()
      withEnvs("SL_ROOT" -> projectDir.pathAsString) {
        assert(new Main().run(Array("bootstrap", "--template", "initializer")))
      }
    }
  }
}
