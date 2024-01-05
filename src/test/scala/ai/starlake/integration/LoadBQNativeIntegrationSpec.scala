package ai.starlake.integration

import ai.starlake.job.Main

class LoadBQNativeIntegrationSpec extends BigQueryIntegrationSpecBase {
  override def templates = starlakeDir / "samples"
  override def localDir = templates / "spark"
  override def sampleDataDir = localDir / "sample-data"
  if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
    "Import / Load / Transform BQ NATIVE" should "succeed" in {
      withEnvs(
        "SL_ROOT"                                       -> localDir.pathAsString,
        "SL_ENV"                                        -> "BQ-NATIVE",
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
    "Import / Load / Transform BQ NATIVE2" should "succeed" in {
      withEnvs(
        "SL_ROOT"                                       -> localDir.pathAsString,
        "SL_ENV"                                        -> "BQ-NATIVE",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC"
      ) {
        val sampleDataDir2 = localDir / "sample-data2"
        sampleDataDir2.copyTo(incomingDir)

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
