package ai.starlake.extract

import java.util.regex.Pattern
import better.files.File
import ai.starlake.TestHelper
import ai.starlake.schema.model._

class TemplateParamsSpec extends TestHelper {
  val scriptOutputFolder: File = File("/tmp")
  new WithSettings() {
    "fromSchema" should "generate the correct TemplateParams for a given Schema" in {
      val schema: Schema = Schema(
        name = "table1",
        pattern = Pattern.compile("output_file.*.csv"),
        List(
          Attribute(name = "col1"),
          Attribute(name = "col2", `type` = "long"),
          Attribute(name = "col3", script = Some("script"))
        ),
        metadata = Option(
          Metadata(
            writeStrategy = Some(
              WriteStrategy(
                `type` = Some(WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP),
                key = List("col1", "col2"),
                timestamp = Some("updateCol")
              )
            )
          )
        ),
        comment = None,
        presql = Nil,
        postsql = Nil
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
        auditDB = None,
        activeEnv = Map.empty
      )
      TemplateParams.fromSchema(
        "AnyDomain",
        schema,
        Some("updateCol"),
        None,
        Map.empty
      ) shouldBe expectedTemplateParams
    }

    it should "generate the correct TemplateParams for an other Schema" in {
      val schema: Schema = Schema(
        name = "table1",
        pattern = Pattern.compile("output_file.*.csv"),
        List(Attribute(name = "col1"), Attribute(name = "col2", `type` = "long")),
        metadata = Option(
          Metadata(
            separator = Some("|"),
            writeStrategy = Some(
              WriteStrategy(
                `type` = Some(WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP),
                key = List("col1", "col2"),
                timestamp = Some("updateCol")
              )
            )
          )
        ),
        comment = None,
        presql = Nil,
        postsql = Nil
      )

      val expectedTemplateParams = TemplateParams(
        domainToExport = "AnyDomain",
        tableToExport = "table1",
        columnsToExport = List(
          ("col1", "string", false, PrivacyLevel.None),
          ("col2", "long", false, PrivacyLevel.None)
        ),
        fullExport = false,
        dsvDelimiter = "|",
        deltaColumn = None,
        auditDB = None,
        activeEnv = Map.empty
      )
      TemplateParams.fromSchema(
        "AnyDomain",
        schema,
        None,
        None,
        Map.empty
      ) shouldBe expectedTemplateParams
    }
  }
}
