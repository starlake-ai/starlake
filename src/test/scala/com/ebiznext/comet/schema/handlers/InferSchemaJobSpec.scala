package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.job.infer.InferSchemaJob
import org.apache.spark.sql.Dataset

class InferSchemaJobSpec extends TestHelper {

  new WithSettings() {

    lazy val dataset_csv: Dataset[String] = sparkSession.read
      .textFile("src/test/resources/sample/SCHEMA-VALID-NOHEADER.dsv")

    lazy val dataset_psv: Dataset[String] = sparkSession.read
      .textFile("src/test/resources/quickstart/incoming/sales/customers-2018-01-01.psv")

    lazy val dataset_json: Dataset[String] = sparkSession.read
      .textFile("src/test/resources/sample/json/complex.json")

    lazy val dataset_jsonArray: Dataset[String] = sparkSession.read
      .textFile("src/test/resources/quickstart/incoming/hr/sellers-2018-01-01.json")

    lazy val dataset_jsonArrayMultiline: Dataset[String] = sparkSession.read
      .textFile("src/test/resources/sample/simple-json-locations/locations.json")

    lazy val inferSchemaJob: InferSchemaJob = new InferSchemaJob()

    "GetSeparatorSemiColon" should "succeed" in {

      inferSchemaJob.getSeparator(dataset_csv) shouldBe ";"

    }

    "GetSeparatorPipe" should "succeed" in {

      inferSchemaJob.getSeparator(dataset_psv) shouldBe "|"

    }

    "GetFormatCSV" should "succeed" in {

      inferSchemaJob.getFormatFile(dataset_csv) shouldBe "DSV"

    }

    "GetFormatJson" should "succeed" in {

      inferSchemaJob.getFormatFile(dataset_json) shouldBe "JSON"

    }

    "GetFormatArrayJson" should "succeed" in {

      inferSchemaJob.getFormatFile(dataset_jsonArray) shouldBe "ARRAY_JSON"

    }

    "GetFormatArrayJsonMultiline" should "succeed" in {

      inferSchemaJob.getFormatFile(dataset_jsonArrayMultiline) shouldBe "ARRAY_JSON"

    }
  }
}
