package ai.starlake.job.bootstrap

import ai.starlake.TestHelper
import better.files.File

class BootstrapCmdSpec extends TestHelper {
  new WithSettings() {
    "BootstrapCmd" should "fail without exiting the JVM when the metadata folder is not empty" in {
      val project = File.newTemporaryDirectory("bootstrap-non-empty")
      try {
        val existing = project / "metadata" / "application.sl.yml"
        existing.createIfNotExists(createParents = true).write("version: 1\n")

        val result = BootstrapCmd.run(
          BootstrapConfig(
            template = Some("empty-project"),
            projectLocation = Some(project.pathAsString)
          ),
          settings.schemaHandler()
        )

        result.isFailure shouldBe true
        result.failed.get shouldBe an[IllegalStateException]
        result.failed.get.getMessage should include("already exists and is not empty")
        existing.contentAsString shouldBe "version: 1\n"
      } finally {
        project.delete(swallowIOExceptions = true)
      }
    }
  }
}
