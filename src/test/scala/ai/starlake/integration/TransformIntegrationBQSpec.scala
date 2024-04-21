package ai.starlake.integration

import ai.starlake.job.Main
import better.files.File

class TransformIntegrationBQSpec extends BigQueryIntegrationSpecBase {
  override def beforeAll(): Unit = {}
  override def templates: File = starlakeDir / "samples"

  override def localDir: File = templates / "spark"

  override def sampleDataDir: File = localDir / "sample-data"

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {

    "Native Bigquery Transform" should "succeed" in {
      withEnvs(
        "SL_ENV"                                        -> "BQ",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
        "SL_ROOT"                                       -> localDir.pathAsString
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
        assert(
          new Main().run(
            Array(
              "transform",
              "--name",
              "sales_kpi.byseller_kpi0"
            )
          )
        )
      }
    }
  }
}
