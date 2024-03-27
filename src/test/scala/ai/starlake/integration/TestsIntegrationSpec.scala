package ai.starlake.integration

import ai.starlake.tests.StarlakeTest
import better.files.File

import java.sql.DriverManager

class TestsIntegrationSpec extends IntegrationTestBase {
  override def templates: File = starlakeDir / "samples"

  override def localDir: File = templates / "spark"

  "Load Tests" should "succeed" in {
    withEnvs(
      "SL_ROOT" -> localDir.pathAsString
    ) {
      val tests = StarlakeTest.load()
      val conn = DriverManager.getConnection("jdbc:duckdb:/Users/hayssams/tmp/duckdb.db")
      StarlakeTest.drop(tests, conn)
      StarlakeTest.run(tests, conn)
    }
  }
}
