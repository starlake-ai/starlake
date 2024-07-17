package ai.starlake.integration.lineage

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File

class LineageIntegrationSpec extends IntegrationTestBase {

  override def templates: File = starlakeDir / "samples"
  override def localDir: File = templates / "spark"
  override def sampleDataDir: File = localDir / "sample-data"

  "Lineage" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString) {
      new Main().run(
        Array("col-lineage", "--task", "sales_kpi.byseller_kpi0")
      )
    }
  }
}
