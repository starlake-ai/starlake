package ai.starlake.integration.utils

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main
import better.files.File

class ProjectCompareIntegrationSpec extends BigQueryIntegrationSpecBase {
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
        assert(
          new Main().run(
            Array(
              "compare",
              "--path1",
              "/Users/hayssams/git/public/starlake/samples/spark",
              "--path2",
              "/Users/hayssams/git/public/starlake/samples/spark2",
              "--output",
              "/Users/hayssams/tmp/output/output.html"
            )
          )
        )
      }
    }
  }
}
