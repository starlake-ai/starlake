package ai.starlake.integration

import ai.starlake.job.Main

class XlsIntegrationSpec extends BigQueryIntegrationSpecBase {
  "Convert to XLS" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> localDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "false"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)
      val loadDir = localDir / "metadata" / "load"

      Main.run(
        Array("yml2xls", "--xls", loadDir.pathAsString)
      )
      (loadDir / "sales.xlsx").exists shouldBe true
      (loadDir / "hr.xlsx").exists shouldBe true
      (loadDir / "sales.xlsx").delete()
      (loadDir / "hr.xlsx").delete()
    }
  }
}
