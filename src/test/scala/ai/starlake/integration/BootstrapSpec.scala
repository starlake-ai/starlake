package ai.starlake.integration

import ai.starlake.job.Main

class BootstrapSpec extends IntegrationTestBase {

  behavior of "BootstrapSpec"
  it should "bootstrap" in {
    val projectDir = starlakeDir / "bootstrap-test"
    projectDir.createDirectoryIfNotExists()
    withEnvs("SL_ROOT" -> projectDir.pathAsString) {
      Main.main(Array("bootstrap", "--template", "bigquery"))
    }
  }
}
