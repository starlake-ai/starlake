package ai.starlake.integration

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.workflow.IngestionWorkflow
import better.files.File
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll

class SchemaHandlerIntegrationSpec extends TestHelper with BeforeAndAfterAll {

  val starlakeDir = File(".")
  logger.info(starlakeDir.pathAsString)
  val localDir = starlakeDir / "samples" / "local"
  val incomingDir = localDir / "incoming"
  val quickstartDir: File = localDir / "quickstart"
  val directoriesToClear = List("incoming", "audit", "datasets", "diagrams")
  setEnv("SL_ROOT", quickstartDir.pathAsString)

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
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {
      clearDataDirectories()
    }
  }

  "Watch single schema" should "load only this schema" in {
    clearDataDirectories()

    implicit val settings: Settings = Settings(ConfigFactory.load())

    val schemaHandler = new SchemaHandler(settings.storageHandler(), Map.empty)
    val workflow =
      new IngestionWorkflow(settings.storageHandler(), schemaHandler)
    assert(schemaHandler.domains(List("hr"), List("locations")).length == 1)
  }

}
