package ai.starlake.integration.utils

import ai.starlake.config.Settings
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.site.{SiteConfig, SiteHandler}
import better.files.File

class SiteHandlerIntegrationSpec extends IntegrationTestBase {

  override val directoriesToClear = List("site")

  val projectDir = theSampleFolder

  protected def clearDataDirectories(): Unit = {
    directoriesToClear.foreach { dir =>
      val path = projectDir / dir
      if (path.exists) {
        path.delete()
      }
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {}
  }

  "Generate standalone site" should "succeed" in {
    withEnvs("SL_ROOT" -> projectDir.pathAsString) {
      clearDataDirectories()
      implicit val settings: Settings = Settings(Settings.referenceConfig, None, None, None, None)
      val schemaHandler = settings.schemaHandler()
      val outputDir = projectDir / "site"
      val config = SiteConfig(outputDir)

      val siteHandler = new SiteHandler(config, schemaHandler)
      siteHandler.run() match {
        case scala.util.Success(_) =>
          logger.info("Standalone site generated successfully")
          assert((outputDir / "index.html").exists)
          assert((outputDir / "style.css").exists)
          assert((outputDir / "flow.js").exists)
          assert((outputDir / "load").exists || (outputDir / "transform").exists)
        case scala.util.Failure(e) =>
          e.printStackTrace()
          logger.error("Site generation failed", e)
          throw e
      }
    }
  }
}
