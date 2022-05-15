package ai.starlake.extractor

import java.util.regex.Pattern
import better.files.File
import ai.starlake.TestHelper
import ai.starlake.schema.model._

class TemplateParamsSpec extends TestHelper {
  val scriptOutputFolder: File = File("/tmp")
  new WithSettings {
    "fromSchema" should "generate the correct TemplateParams for a given Schema" in {
      val schema: Schema = Schema(
        name = "table1",
        pattern = Pattern.compile("output_file.*.csv"),
        List(
          Attribute(name = "col1"),
          Attribute(name = "col2", `type` = "long"),
          Attribute(name = "col3", script = Some("script"))
        ),
        metadata = Option(Metadata(write = Some(WriteMode.APPEND))),
        merge = Some(MergeOptions(List("col1", "col2"), None, timestamp = Some("updateCol"))),
        comment = None,
        presql = None,
        postsql = None
      )

      val expectedTemplateParams = TemplateParams(
        domainToExport = "AnyDomain",
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
      TemplateParams.fromSchema(
        "AnyDomain",
        schema,
        scriptOutputFolder,
        None,
        Some("updateCol"),
        Map.empty
      ) shouldBe expectedTemplateParams
    }

    it should "generate the correct TemplateParams for an other Schema" in {
      val schema: Schema = Schema(
        name = "table1",
        pattern = Pattern.compile("output_file.*.csv"),
        List(Attribute(name = "col1"), Attribute(name = "col2", `type` = "long")),
        metadata = Option(Metadata(write = Some(WriteMode.OVERWRITE), separator = Some("|"))),
        merge = Some(MergeOptions(List("col1", "col2"), None, timestamp = Some("updateCol"))),
        comment = None,
        presql = None,
        postsql = None
      )

      val expectedTemplateParams = TemplateParams(
        domainToExport = "AnyDomain",
        tableToExport = "table1",
        columnsToExport = List(
          ("col1", "string", false, PrivacyLevel.None),
          ("col2", "long", false, PrivacyLevel.None)
        ),
        fullExport = true,
        dsvDelimiter = "|",
        deltaColumn = None,
        exportOutputFileBase = "output_file",
        scriptOutputFile = Some(scriptOutputFolder / "extract_AnyDomain.table1.sql"),
        activeEnv = Map.empty
      )
      TemplateParams.fromSchema(
        "AnyDomain",
        schema,
        scriptOutputFolder,
        None,
        None,
        Map.empty
      ) shouldBe expectedTemplateParams
    }
  }
}
