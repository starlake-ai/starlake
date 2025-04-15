package ai.starlake.integration.utils

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main

class XlsIntegrationSpec extends BigQueryIntegrationSpecBase {
  "Convert to XLS" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> theSampleFolder.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "false"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)
      val loadDir = theSampleFolder / "metadata" / "load"

      assert(
        new Main().run(
          Array("yml2xls", "--xls", loadDir.pathAsString)
        )
      )
      (loadDir / "sales.xlsx").exists shouldBe true
      (loadDir / "hr.xlsx").exists shouldBe true
      (loadDir / "sales.xlsx").delete()
      (loadDir / "hr.xlsx").delete()
    }
  }
}
