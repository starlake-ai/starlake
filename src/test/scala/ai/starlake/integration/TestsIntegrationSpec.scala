package ai.starlake.integration

import ai.starlake.job.Main
import better.files.File

class TestsIntegrationSpec extends IntegrationTestBase {
  override def templates: File = starlakeDir / "samples"

  override def localDir: File = templates / "spark"

  "Load Tests" should "succeed" in {
    withEnvs(
      "SL_ROOT" -> localDir.pathAsString
    ) {
      new Main().run(Array("test"))
    }
  }
}
