package ai.starlake.integration.utils

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File

class TestsIntegrationSpec extends IntegrationTestBase {
  override def templates: File = starlakeDir / "samples"

  override def localDir: File = File("/Users/tiboun/starlake-unit-testing")

  "Load Tests" should "succeed" in {
    withEnvs(
      "SL_ROOT" -> localDir.pathAsString
    ) {
      new Main().run(Array("test"))
    }
  }
}
