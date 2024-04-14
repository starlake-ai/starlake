package ai.starlake.integration

import ai.starlake.job.Main
import better.files.File

class TranspileIntegrationBQSpec extends BigQueryIntegrationSpecBase {
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
              "transform",
              "--compile",
              "--name",
              "sales_kpi.byseller_kpi0",
              "--accessToken",
              "ya29.a0Ad52N3_uL--Zg9AoutEfC4GE7hs-CERpMZXhnrlqddl-FiXa1oxOzxbDkyqEI2PmfVQ3bku3Gw5oZL5ZsKdkIdtspPhDER0XOpSCwvYL9TagphsGZkzAbJ3eAeWU2GncH5XfSo1ptqA_YRDegpWIclu9F9ckEXz6rWhFfWhpKBaNhQaCgYKAVUSARASFQHGX2Miymf730chwqrafHc3l9vkxg0181"
            )
          )
        )
      }
    }
  }
}
