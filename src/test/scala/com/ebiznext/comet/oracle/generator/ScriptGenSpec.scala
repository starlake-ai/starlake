package com.ebiznext.comet.oracle.generator

import better.files.File
import org.scalatest.matchers.should.Matchers

class ScriptGenSpec extends org.scalatest.FlatSpec with Matchers {

  val scriptOutputFolder: File = File("/tmp")

  "templatize" should "generate an export script from a TemplateSettings" in {
    val templateParams: TemplateParams = TemplateParams(
      tableToExport = "table1",
      columnsToExport = List("col1", "col2"),
      isDelta = true,
      dsvDelimiter = ",",
      deltaColumn = Some("updateCol"),
      exportOutputFileBase = "output_file",
      scriptOutputFile = scriptOutputFolder / "EXTRACT_table1.sql"
    )

    val templatePayload: String = ScriptGen.templatize(
      File(
        getClass.getResource("/sample/oracle/EXTRACT_TABLE.sql.mustache").getPath
      ),
      templateParams
    )

    templatePayload shouldBe File(
      getClass.getResource("/sample/oracle/expected_script_payload.txt").getPath
    ).lines.mkString("\n")
  }

}
