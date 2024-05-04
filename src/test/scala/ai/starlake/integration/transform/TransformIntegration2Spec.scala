package ai.starlake.integration.transform

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main

class TransformIntegration2Spec extends BigQueryIntegrationSpecBase {
  if (
    sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean && sys.env
      .getOrElse("SL_REMOTE_TEST", "false")
      .toBoolean
  ) {

    "Import / Load / Transform BQ" should "succeed" in {
      withEnvs(
        "SL_ROOT" -> localDir.pathAsString,
        "SL_ENV"  -> "BQ"
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        assert(
          new Main().run(
            Array("transform", "--name", "bqtest.table1")
          )
        )
      }
    }
  }
}
