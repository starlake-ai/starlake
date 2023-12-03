package ai.starlake.integration

import ai.starlake.job.Main

class TransformIntegration2Spec extends BigQueryIntegrationSpecBase {
  if (
    sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean && sys.env
      .getOrElse("SL_GCP_TEST", "false")
      .toBoolean
  ) {

    "Import / Load / Transform BQ" should "succeed" in {
      withEnvs(
        "SL_ROOT" -> localDir.pathAsString,
        "SL_ENV"  -> "BQ"
      ) {
        clearDataDirectories()
        sampleDataDir.copyToDirectory(localDir)
        Main.main(
          Array("transform", "--name", "bqtest.table1")
        )
      }
    }
  }
}
