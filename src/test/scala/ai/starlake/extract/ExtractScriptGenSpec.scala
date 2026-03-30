package ai.starlake.extract

import ai.starlake.TestHelper
import ai.starlake.schema.model.TransformInput
import better.files.File

class ExtractScriptGenSpec extends TestHelper {

  val scriptOutputFolder: File = File(System.getProperty("java.io.tmpdir"))
  new WithSettings() {

    "templatize domain using j2" should "generate an export script from a TemplateSettings" in {
      val templateParams: TemplateParams = TemplateParams(
        domainToExport = "domain1",
        tableToExport = "table1",
        columnsToExport = List(
          ("col1", "string", false, TransformInput.None),
          ("col2", "long", false, TransformInput.None),
          ("col3", "string", true, TransformInput.None),
          ("col4", "string", false, TransformInput.None)
        ),
        fullExport = false,
        dsvDelimiter = ",",
        deltaColumn = Some("updateCol"),
        auditDB = None,
        activeEnv = Map.empty
      )

      val templatePayload: String = new ExtractScript(settings.schemaHandler())
        .templatizeFile(
          File(getClass.getResource("/sample/database/EXTRACT_TABLE.sql.j2")).pathAsString,
          templateParams
        )
        .pathAsString

      def normalize(s: String): String =
        s.toLowerCase.replaceAll("\\s+", " ").trim
      print(getClass.getResource("/sample/database/expected_script_payload2.txt").getPath)
      normalize(File(templatePayload).contentAsString) shouldBe normalize(
        File(getClass.getResource("/sample/database/expected_script_payload2.txt")).contentAsString
      )
    }
  }
}
