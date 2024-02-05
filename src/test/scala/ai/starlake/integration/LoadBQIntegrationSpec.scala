package ai.starlake.integration

import ai.starlake.job.Main
import better.files.File

class LoadBQIntegrationSpec extends BigQueryIntegrationSpecBase {
  override def templates: File = starlakeDir / "samples"
  override def localDir: File = templates / "spark"
  override def sampleDataDir: File = localDir / "sample-data"
  if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
    "Import / Load / Transform BQ" should "succeed" in {
      withEnvs(
        "SL_ROOT"                                   -> localDir.pathAsString,
        "SL_ENV"                                    -> "BQ",
        "SL_SPARK_BIGQUERY_MATERIALIZATION_DATASET" -> "SL_BQ_TEST_DS"
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
    "Import / Load / Transform BQ 2" should "succeed" in {
      withEnvs(
        "SL_ROOT"                                   -> localDir.pathAsString,
        "SL_ENV"                                    -> "BQ",
        "SL_SPARK_BIGQUERY_MATERIALIZATION_DATASET" -> "SL_BQ_TEST_DS"
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
