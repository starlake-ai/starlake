package ai.starlake.extractor

import ai.starlake.schema.model.PrivacyLevel
import better.files.File
import ai.starlake.TestHelper
import ai.starlake.schema.handlers.{SchemaHandler, SimpleLauncher}
import ai.starlake.schema.model.PrivacyLevel

class ExtractScriptGenSpec extends TestHelper {

  val scriptOutputFolder: File = File("/tmp")
  new WithSettings {

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

      val templatesPayloadFromDir = new ScriptGen(
        storageHandler,
        new SchemaHandler(settings.storageHandler),
        new SimpleLauncher
      ).templatize(
        File(
          getClass.getResource("/sample/database").getPath
        ),
        templateParams
      ).head
        .pathAsString

      println(File(templatesPayloadFromDir).lines.mkString("\n").toLowerCase)

      val templatePayload = new ScriptGen(
        storageHandler,
        new SchemaHandler(settings.storageHandler),
        new SimpleLauncher
      ).templatize(
        File(
          getClass.getResource("/sample/database/EXTRACT_TABLE.sql.mustache").getPath
        ),
        templateParams
      ).head
        .pathAsString

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

      val templatePayload: String = new ScriptGen(
        storageHandler,
        new SchemaHandler(settings.storageHandler),
        new SimpleLauncher
      ).templatize(
        File(
          getClass.getResource("/sample/database/EXTRACT_TABLE.sql.ssp").getPath
        ),
        templateParams
      ).head
        .pathAsString

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

        val config = ExtractScriptGenConfig(
          jobs = List("my-job"),
          scriptOutputDir = scriptOutputFolder,
          scriptOutputPattern = Some("comet-test-my-job.txt"),
          scriptTemplateFile = File(getClass.getResource("/sample/job/extract-job.ssp").getPath)
        )
        val success = new ScriptGen(
          storageHandler,
          new SchemaHandler(settings.storageHandler),
          new SimpleLauncher
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
