package ai.starlake.integration

import ai.starlake.job.Main
import better.files.File

class ExtractBqSchemaSpec extends IntegrationTestBase {
  if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
    val starlakeDir = File(".")
    logger.info(starlakeDir.pathAsString)
    val localDir = starlakeDir / "samples" / "local"
    val quickstartDir: File = localDir / "quickstart"

    "Extract sales_kpi" should "create yaml file in external" in {
      withEnvs("SL_ROOT" -> quickstartDir.pathAsString, "SL_ENV" -> "BQ") {
        Main.main(
          Array("extract-bq-schema", "--tables", "sales_kpi")
        )
      }
    }
  }
}
