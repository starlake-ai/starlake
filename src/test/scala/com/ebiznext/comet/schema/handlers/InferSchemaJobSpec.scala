package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.job.infer.InferSchemaJob

import scala.io.Source

class InferSchemaJobSpec extends TestHelper {

  new WithSettings() {

    lazy val csvLines =
      Source.fromFile("src/test/resources/sample/SCHEMA-VALID-NOHEADER.dsv").getLines().toList

    lazy val psvLines = Source
      .fromFile("src/test/resources/quickstart/incoming/sales/customers-2018-01-01.psv")
      .getLines()
      .toList

    lazy val jsonLines =
      Source.fromFile("src/test/resources/sample/json/complex.json").getLines().toList

    lazy val jsonArrayLines = Source
      .fromFile("src/test/resources/quickstart/incoming/hr/sellers-2018-01-01.json")
      .getLines()
      .toList

    lazy val jsonArrayMultilinesLines =
      Source
        .fromFile("src/test/resources/sample/simple-json-locations/locations.json")
        .getLines()
        .toList

    lazy val xmlLines =
      Source.fromFile("src/test/resources/sample/xml/locations.xml").getLines().toList

    lazy val inferSchemaJob: InferSchemaJob = new InferSchemaJob()

    "GetSeparatorSemiColon" should "succeed" in {
      inferSchemaJob.getSeparator(csvLines) shouldBe ";"
    }

    "GetSeparatorPipe" should "succeed" in {
      inferSchemaJob.getSeparator(psvLines) shouldBe "|"
    }

    "GetFormatCSV" should "succeed" in {
      inferSchemaJob.getFormatFile(csvLines) shouldBe "DSV"
    }

    "GetFormatJson" should "succeed" in {
      inferSchemaJob.getFormatFile(jsonLines) shouldBe "JSON"
    }

    "GetFormatXML" should "succeed" in {
      inferSchemaJob.getFormatFile(xmlLines) shouldBe "XML"
    }

    "GetFormatArrayJson" should "succeed" in {
      inferSchemaJob.getFormatFile(jsonArrayLines) shouldBe "ARRAY_JSON"
    }

    "GetFormatArrayJsonMultiline" should "succeed" in {

      inferSchemaJob.getFormatFile(jsonArrayMultilinesLines) shouldBe "ARRAY_JSON"

    }
  }
}
