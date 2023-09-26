package ai.starlake.integration

import ai.starlake.job.Main

class XlsIntegrationSpec extends BigQueryIntegrationSpecBase {
  "Convert to XLS" should "succeed" in {
    pending
    withEnvs(
      "SL_ROOT"                     -> quickstartDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "false"
    ) {
      clearDataDirectories()
      incomingDir.copyToDirectory(quickstartDir)
      val loadDir = quickstartDir / "metadata" / "load"

      Main.main(
        Array("yml2xls", "--xls", loadDir.pathAsString)
      )
    }
  }
}
