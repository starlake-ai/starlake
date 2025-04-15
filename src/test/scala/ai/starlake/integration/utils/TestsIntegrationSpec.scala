package ai.starlake.integration.utils

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class TestsIntegrationSpec extends IntegrationTestBase {
  "Load Tests" should "succeed" in {
    withEnvs(
      "SL_ROOT" -> theSampleFolder.pathAsString
    ) {
      new Main().run(Array("test", "--load"))
    }
  }
}
