package ai.starlake.integration

import ai.starlake.job.Main
import better.files.File

class LoadPgIntegrationSpec extends JDBCIntegrationSpecBase {
  override def templates: File = starlakeDir / "samples"
  override def localDir: File = templates / "spark"
  override def sampleDataDir: File = localDir / "sample-data"

  if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
    "Import / Load / Transform PG" should "succeed" in {
      withEnvs(
        "SL_ROOT"                                       -> localDir.pathAsString,
        "SL_ENV"                                        -> "PG",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC"
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        Main.main(
          Array("import")
        )
        Main.main(
          Array("load")
        )
      }
    }
    "Import / Load / Transform PG 2" should "succeed" in {
      withEnvs(
        "SL_ROOT"                                       -> localDir.pathAsString,
        "SL_ENV"                                        -> "PG",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC"
      ) {
        val sampleDataDir2 = localDir / "sample-data2"
        copyFilesToIncomingDir(sampleDataDir2)
        Main.main(
          Array("import")
        )
        Main.main(
          Array("load")
        )
      }
    }
  }
}
