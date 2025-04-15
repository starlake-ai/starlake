package ai.starlake.integration.transform

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main

class TransformIntegrationRedshiftSpec extends BigQueryIntegrationSpecBase {
  if (false && sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {

    "Native REDSHIFT Transform" should "succeed" in {
      withEnvs(
        "SL_ENV"  -> "REDSHIFT",
        "SL_ROOT" -> theSampleFolder.pathAsString
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        assert(
          new Main().run(
            Array("transform", "--name", "sales_kpi.byseller_kpi")
          )
        )
      }
    }
  }
}
