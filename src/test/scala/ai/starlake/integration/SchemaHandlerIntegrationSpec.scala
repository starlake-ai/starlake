package ai.starlake.integration

import ai.starlake.TestHelper
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.workflow.IngestionWorkflow
import better.files.File

class SchemaHandlerIntegrationSpec extends TestHelper {

  val starlakeDir = File(".")
  logger.info(starlakeDir.pathAsString)
  val localDir = starlakeDir / "samples" / "local"
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

  override def afterAll(): Unit = {
    super.afterAll()
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      clearDataDirectories()
    }
  }

  "Watch single schema" should "load only this schema" in {
    // It works locally but not in pipeline. Wrapping it in order to use it only locally
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs("SL_ROOT" -> quickstartDir.pathAsString) {
        new WithSettings() {
          clearDataDirectories()
          val schemaHandler = new SchemaHandler(settings.storageHandler(), Map.empty)
          val workflow =
            new IngestionWorkflow(settings.storageHandler(), schemaHandler)
          assert(schemaHandler.domains(List("hr"), List("locations")).length == 1)
        }
      }
    }
  }

}
