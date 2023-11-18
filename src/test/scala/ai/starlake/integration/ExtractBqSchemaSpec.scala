package ai.starlake.integration

import ai.starlake.job.Main

class ExtractBqSchemaSpec extends IntegrationTestBase {
  if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
    "Extract sales_kpi" should "create yaml file in external" in {
      withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "BQ") {
        Main.main(
          Array("extract-bq-schema", "--tables", "sales_kpi")
        )
      }
    }
  }
}
