package ai.starlake.integration.load

import ai.starlake.integration.JDBCIntegrationSpecBase
import ai.starlake.job.Main
import better.files.File

class LoadDuckDbIntegrationSpec extends JDBCIntegrationSpecBase {
  override def templates: File = starlakeDir / "samples"
  override def localDir: File = starlakeDir / "samples" / "duckdb"
  override def sampleDataDir: File = localDir / "sample-data"

  override protected def cleanup(): Unit = {
    // cleanup(localDir)
  }

  if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
    "Import / Load / Transform DUCKDB" should "succeed" in {
      (localDir / "datasets").createDirectoryIfNotExists(createParents = true)
      withEnvs(
        "SL_ROOT" -> localDir.pathAsString,
        "SL_ENV"  -> "DUCKDB"
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        assert(
          new Main().run(
            Array("import")
          )
        )
        assert(
          new Main().run(
            Array("load")
          )
        )
      }
    }
  }
}
