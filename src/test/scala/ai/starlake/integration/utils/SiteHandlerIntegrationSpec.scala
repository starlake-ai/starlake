package ai.starlake.integration.utils

import ai.starlake.config.Settings
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.site.{SiteConfig, SiteHandler}
import better.files.File

class SiteHandlerIntegrationSpec extends IntegrationTestBase {

  val starbakeDir = File(System.getProperty("user.home") + "/git/starbake")
  override val directoriesToClear = List("site")

  // select quickstart or starbake here
  val projectDir = theSampleFolder
  // val projectDir = starbakeDir

  // select docusaurus folder
  val docusaurusFolder =
    File(System.getProperty("user.home") + "/tmp/docusaurus/my-website/docs")

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

  "Generate Docusaurus site" should "succeed" in {
    // select quickstart or starbake here
    withEnvs("SL_ROOT" -> projectDir.pathAsString) {
      clearDataDirectories()
      implicit val settings: Settings = Settings(Settings.referenceConfig, None, None)
      val schemaHandler = settings.schemaHandler()
      val config = SiteConfig(
        docusaurusFolder,
        templateName = Some("docusaurus")
      )

      val siteHandler = new SiteHandler(config, schemaHandler)
      siteHandler.run() match {
        case scala.util.Success(_) => logger.info("Site generated successfully")
        case scala.util.Failure(e) =>
          e.printStackTrace()
          logger.error("Site generation failed", e)
          throw e
      }
    }
  }
}
