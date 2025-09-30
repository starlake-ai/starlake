package ai.starlake.integration.transform

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main

class TransformIntegrationBQSpec extends BigQueryIntegrationSpecBase {
  override def beforeAll(): Unit = {}

  "Native Bigquery Load" should "succeed" in {
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      withEnvs(
        "SL_ENV"                                        -> "BQ",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
        "SL_ROOT"                                       -> theSampleFolder.pathAsString
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        assert(
          new Main().run(
            Array("stage")
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
  "Native Bigquery Transform" should "succeed" in {
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      withEnvs(
        "SL_ENV"                                        -> "BQ",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
        "SL_ROOT"                                       -> theSampleFolder.pathAsString
      ) {
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
