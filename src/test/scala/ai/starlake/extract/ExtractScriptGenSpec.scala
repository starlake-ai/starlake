package ai.starlake.extract

import ai.starlake.TestHelper
import ai.starlake.schema.handlers.{SchemaHandler, SimpleLauncher}
import ai.starlake.schema.model.PrivacyLevel
import better.files.File

class ExtractScriptGenSpec extends TestHelper {

  val scriptOutputFolder: File = File("/tmp")
  new WithSettings() {

    "templatize domain using mustache" should "generate an export script from a TemplateSettings" in {
      val templateParams: TemplateParams = TemplateParams(
        domainToExport = "domain1",
        tableToExport = "table1",
        columnsToExport = List(
          ("col1", "string", false, PrivacyLevel.None),
          ("col2", "long", false, PrivacyLevel.None)
        ),
        fullExport = false,
        dsvDelimiter = ",",
        deltaColumn = Some("updateCol"),
        exportOutputFileBase = "output_file",
        scriptOutputFile = Some(scriptOutputFolder / "extract_AnyDomain.table1.sql"),
        activeEnv = Map.empty
      )

      val templatesPayloadFromDir = new ExtractScript(
        storageHandler,
        new SchemaHandler(settings.storageHandler),
        new SimpleLauncher()
      ).templatizeFolder(
        File(
          getClass.getResource("/sample/database").getPath
        ),
        templateParams
      ).head
        .pathAsString

      println(File(templatesPayloadFromDir).lines.mkString("\n").toLowerCase)

      val templatePayload = new ExtractScript(
        storageHandler,
        new SchemaHandler(settings.storageHandler),
        new SimpleLauncher()
      ).templatizeFile(
        getClass.getResource("/sample/database/EXTRACT_TABLE.sql.mustache").getPath,
        templateParams
      ).pathAsString

      File(templatePayload).lines.mkString("\n").toLowerCase shouldBe File(
        getClass.getResource("/sample/database/expected_script_payload.txt").getPath
      ).lines.mkString("\n").toLowerCase
    }

    "templatize domain using ssp" should "generate an export script from a TemplateSettings" in {
      val templateParams: TemplateParams = TemplateParams(
        domainToExport = "domain1",
        tableToExport = "table1",
        columnsToExport = List(
          ("col1", "string", false, PrivacyLevel.None),
          ("col2", "long", false, PrivacyLevel.None),
          ("col3", "string", true, PrivacyLevel.None),
          ("col4", "string", false, PrivacyLevel.None)
        ),
        fullExport = false,
        dsvDelimiter = ",",
        deltaColumn = Some("updateCol"),
        exportOutputFileBase = "output_file",
        scriptOutputFile = Some(scriptOutputFolder / "EXTRACT_TABLE.sql"),
        activeEnv = Map.empty
      )

      val templatePayload: String = new ExtractScript(
        storageHandler,
        new SchemaHandler(settings.storageHandler),
        new SimpleLauncher()
      ).templatizeFile(
        getClass.getResource("/sample/database/EXTRACT_TABLE.sql.ssp").getPath,
        templateParams
      ).pathAsString

      print(getClass.getResource("/sample/database/expected_script_payload2.txt").getPath)
      File(templatePayload).lines.mkString("\n").toLowerCase shouldBe File(
        getClass.getResource("/sample/database/expected_script_payload2.txt").getPath
      ).lines.mkString("\n").toLowerCase
    }

    "templatize job using ssp" should "generate an export script from a TemplateSettings" in {
      new SpecTrait(
        domainOrJobFilename = "my-job.comet.yml",
        sourceDomainOrJobPathname = s"/sample/job/my-job.comet.yml",
        datasetDomainName = "my-job",
        sourceDatasetPathName = "Ignore", // ot accessed since not loading pending files
        isDomain = false
      ) {
        cleanMetadata
        cleanDatasets

        val config = ExtractScriptConfig(
          scriptOutputDir = scriptOutputFolder,
          scriptOutputPattern = Some("comet-test-my-job.txt"),
          scriptTemplateName = getClass.getResource("/sample/job").getPath
        )
        val success = new ExtractScript(
          storageHandler,
          new SchemaHandler(settings.storageHandler),
          new SimpleLauncher()
        ).run(config)(settings)
        assert(success)

        val resultFile = scriptOutputFolder / "comet-test-my-job.txt"
        logger.info(resultFile.contentAsString)
        resultFile.contentAsString.trim shouldBe File(
          getClass.getResource("/sample/job/expected-extract-job.txt").getPath
        ).contentAsString.trim
      }
    }
  }
}
