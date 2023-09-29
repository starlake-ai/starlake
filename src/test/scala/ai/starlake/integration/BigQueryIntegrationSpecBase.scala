package ai.starlake.integration

import ai.starlake.TestHelper
import better.files.File

class BigQueryIntegrationSpecBase extends TestHelper {

  val starlakeDir = File(".")
  logger.info(starlakeDir.pathAsString)
  val localDir = starlakeDir / "samples" / "local"
  val incomingDir = localDir / "incoming"
  val quickstartDir: File = localDir / "quickstart"
  val directoriesToClear = List("incoming", "audit", "datasets", "diagrams")

  protected def clearDataDirectories(): Unit = {
    directoriesToClear.foreach { dir =>
      val path = quickstartDir / dir
      if (path.exists) {
        path.delete()
      }
    }
  }

  override def beforeAll(): Unit = {
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {}
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      clearDataDirectories()
    }
  }
}
