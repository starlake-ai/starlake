package ai.starlake.integration.utils

import ai.starlake.config.Settings
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.workflow.IngestionWorkflow

class SchemaInfoHandlerIntegrationSpec extends IntegrationTestBase {

  protected def clearDataDirectories(): Unit = {
    directoriesToClear.foreach { dir =>
      val path = theSampleFolder / dir
      if (path.exists) {
        path.delete()
      }
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      clearDataDirectories()
    }
  }

  "Watch single schema" should "load only this schema" in {
    // It works locally but not in pipeline. Wrapping it in order to use it only locally
    withEnvs("SL_ROOT" -> theSampleFolder.pathAsString) {
      clearDataDirectories()
      implicit val settings: Settings = Settings(Settings.referenceConfig, None, None, None)
      val schemaHandler = settings.schemaHandler()
      val workflow =
        new IngestionWorkflow(settings.storageHandler(), schemaHandler)
      assert(schemaHandler.domains(List("hr"), List("flat_locations")).length == 1)
    }
  }

}
