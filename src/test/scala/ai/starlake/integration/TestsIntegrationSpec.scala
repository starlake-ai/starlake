package ai.starlake.integration

import ai.starlake.tests.StarlakeTestData
import better.files.File

class TestsIntegrationSpec extends IntegrationTestBase {
  override def templates: File = starlakeDir / "samples"

  override def localDir: File = templates / "spark"

  "Load Tests" should "succeed" in {
    withEnvs(
      "SL_ROOTdocker create " -> localDir.pathAsString
    ) {

      val tests = StarlakeTestData.loadTests()
      StarlakeTestData.run(tests)
    }
  }
}
