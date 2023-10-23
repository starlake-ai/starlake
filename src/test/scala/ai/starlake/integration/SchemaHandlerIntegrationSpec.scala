package ai.starlake.integration

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.workflow.IngestionWorkflow
import better.files.File
import com.typesafe.config.ConfigFactory

class SchemaHandlerIntegrationSpec extends IntegrationTestBase {

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
    withEnvs("SL_ROOT" -> quickstartDir.pathAsString) {
      clearDataDirectories()
      implicit val settings: Settings = Settings(ConfigFactory.load())
      val schemaHandler = new SchemaHandler(settings.storageHandler(), Map.empty)
      val workflow =
        new IngestionWorkflow(settings.storageHandler(), schemaHandler)
      assert(schemaHandler.domains(List("hr"), List("locations")).length == 1)
    }
  }

}
