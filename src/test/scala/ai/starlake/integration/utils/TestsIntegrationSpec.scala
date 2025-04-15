package ai.starlake.integration.utils

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File

class TestsIntegrationSpec extends IntegrationTestBase {
  override def samplesFolder: File = starlakeDir / "samples"

  override def theSampleFolder: File = samplesFolder / "spark"

  "Load Tests" should "succeed" in {
    withEnvs(
      "SL_ROOT" -> theSampleFolder.pathAsString
    ) {
      new Main().run(Array("test", "--load"))
    }
  }
}
