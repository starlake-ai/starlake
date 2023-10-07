package ai.starlake.integration

import ai.starlake.config.Settings
import ai.starlake.job.site.{SiteConfig, SiteHandler}
import ai.starlake.schema.handlers.SchemaHandler
import better.files.File
import com.typesafe.config.ConfigFactory

class SiteHandlerIntegrationSpec extends IntegrationTestBase {

  val starlakeDir = File(".")
  logger.info(starlakeDir.pathAsString)
  val localDir = starlakeDir / "samples" / "local"
  val quickstartDir: File = localDir / "quickstart"
  val starbakeDir = File(System.getProperty("user.home") + "/git/starbake")
  val directoriesToClear = List("site")

  // select quickstart or starbake here
  val projectDir = quickstartDir
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
    if (sys.env.getOrElse("SL_GCP_TEST", "false").toBoolean) {}
  }

  "Generate Docusaurus site" should "succeed" in {
    // select quickstart or starbake here
    withEnvs("SL_ROOT" -> projectDir.pathAsString) {
      // withEnvs("SL_ROOT" -> projectDir.pathAsString, "SL_METADATA" -> projectDir.pathAsString) {
      clearDataDirectories()
      implicit val settings: Settings = Settings(ConfigFactory.load())
      val schemaHandler = new SchemaHandler(settings.storageHandler(), Map.empty)
      val config = SiteConfig(
        docusaurusFolder,
        templateName = Some("docusaurus")
      )

      val siteHandler = new SiteHandler(config, schemaHandler)
      siteHandler.run()
    }
  }
}
