package com.ebiznext.comet.extractor

import better.files.File
import com.ebiznext.comet.TestHelper

class ExtractScriptGenSpec extends TestHelper {

  val scriptOutputFolder: File = File("/tmp")

  "templatize domain using mustache" should "generate an export script from a TemplateSettings" in {
    val templateParams: TemplateParams = TemplateParams(
      domainToExport = "domain1",
      tableToExport = "table1",
      columnsToExport = List("col1" -> "string", "col2" -> "long"),
      fullExport = false,
      dsvDelimiter = ",",
      deltaColumn = Some("updateCol"),
      exportOutputFileBase = "output_file",
      scriptOutputFile = scriptOutputFolder / "EXTRACT_table1.sql"
    )

    val templatePayload: String = ScriptGen.templatize(
      File(
        getClass.getResource("/sample/database/EXTRACT_TABLE.sql.mustache").getPath
      ),
      templateParams
    )

    templatePayload shouldBe File(
      getClass.getResource("/sample/database/expected_script_payload.txt").getPath
    ).lines.mkString("\n")
  }

  new WithSettings() {
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
        val success = ScriptGen.run(config)(settings)
        assert(success)

        val resultFile = scriptOutputFolder / "comet-test-my-job.txt"
        resultFile.contentAsString.trim shouldBe File(
          getClass.getResource("/sample/job/expected-extract-job.txt").getPath
        ).contentAsString.trim
      }
    }
  }
}
