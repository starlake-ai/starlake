package ai.starlake.integration.load

import ai.starlake.TestHelper
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File

import scala.reflect.io.Directory

class InferSchemaInfoXMLIntegrationSpec extends IntegrationTestBase with TestHelper {
  override def sampleDataDir = theSampleFolder / "sample-data"
  override def beforeEach(): Unit = {
    super.beforeEach()
  }
  override def afterEach(): Unit = {
    super.afterEach()
  }

  "Infer Schema XML" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> theSampleFolder.pathAsString,
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_ENV"                      -> "LOCAL"
    ) {
      new Directory(new java.io.File(settings.appConfig.datasets)).deleteRecursively()

      copyFilesToIncomingDir(sampleDataDir)
      assert(
        new Main().run(
          Array(
            "infer-schema",
            "--clean",
            "--domain",
            "books",
            "--table",
            "items",
            "--format",
            "XML",
            "--input",
            File(sampleDataDir, "books/items.xml").pathAsString
          )
        )
      )
    }
  }
}
