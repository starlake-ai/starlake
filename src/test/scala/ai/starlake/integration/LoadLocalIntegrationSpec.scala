package ai.starlake.integration

import ai.starlake.job.Main

class LoadLocalIntegrationSpec extends IntegrationTestBase {
  override def templates = starlakeDir / "samples"
  override def localDir = templates / "spark"
  override val incomingDir = localDir / "incoming"
  override def sampleDataDir = localDir / "sample-data"
  "Import / Load / Transform Local" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> localDir.pathAsString,
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_ENV"                      -> "LOCAL"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)
      Main.main(
        Array("import")
      )
      Main.main(
        Array("load")
      )
    }
  }
  "Import / Load / Transform Local 2" should "succeed" in {
    withEnvs(
      "SL_ROOT" -> localDir.pathAsString,
      "SL_ENV"  -> "LOCAL"
    ) {
      val sampleDataDir2 = localDir / "sample-data2"
      copyFilesToIncomingDir(sampleDataDir2)
      Main.main(
        Array("import")
      )
      Main.main(
        Array("load")
      )
    }
  }
}
