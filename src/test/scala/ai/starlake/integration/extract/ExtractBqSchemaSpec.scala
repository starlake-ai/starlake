package ai.starlake.integration.extract

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class ExtractBqSchemaSpec extends IntegrationTestBase {
  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "Extract sales_kpi" should "create yaml file in external" in {
      withEnvs("SL_ROOT" -> theSampleFolder.pathAsString, "SL_ENV" -> "BQ") {
        assert(
          new Main().run(
            Array(
              "extract-bq-schema",
              "--tables",
              "sales_kpi"
            )
          )
        )
      }
    }
  }
}
