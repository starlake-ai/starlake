package ai.starlake.integration.transform

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main

class TransformIntegration2Spec extends BigQueryIntegrationSpecBase {
  if (
    sys.env
      .getOrElse("SL_REMOTE_TEST", "true")
      .toBoolean
  ) {

    "Import / Load / Transform BQ" should "succeed" in {
      withEnvs(
        "SL_ROOT" -> theSampleFolder.pathAsString,
        "SL_ENV"  -> "BQ"
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        // bqtest_it2 is this suite's own dataset: no other suite reads or writes it
        assert(
          new Main().run(
            Array("transform", "--name", "bqtest_it2.table1")
          )
        )
      }
    }
  }
}
