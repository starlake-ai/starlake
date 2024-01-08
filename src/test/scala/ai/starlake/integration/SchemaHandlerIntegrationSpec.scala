package ai.starlake.integration

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.workflow.IngestionWorkflow

class SchemaHandlerIntegrationSpec extends IntegrationTestBase {

  protected def clearDataDirectories(): Unit = {
    directoriesToClear.foreach { dir =>
      val path = localDir / dir
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
    withEnvs("SL_ROOT" -> localDir.pathAsString) {
      clearDataDirectories()
      implicit val settings: Settings = Settings(Settings.referenceConfig)
      val schemaHandler = new SchemaHandler(settings.storageHandler(), Map.empty)
      val workflow =
        new IngestionWorkflow(settings.storageHandler(), schemaHandler)
      assert(schemaHandler.domains(List("hr"), List("locations")).length == 1)
    }
  }

}
