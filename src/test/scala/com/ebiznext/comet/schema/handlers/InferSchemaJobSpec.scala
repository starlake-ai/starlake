package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.job.infer.InferSchemaJob
import org.apache.spark.sql.Dataset

class InferSchemaJobSpec extends TestHelper {

  val dataset_csv: Dataset[String] = sparkSession.read
    .textFile("src/test/resources/sample/SCHEMA-VALID-NOHEADER.dsv")

  val dataset_psv: Dataset[String] = sparkSession.read
    .textFile("src/test/resources/quickstart/incoming/sales/customers-2018-01-01.psv")

  val dataset_json: Dataset[String] = sparkSession.read
    .textFile("src/test/resources/sample/json/complex.json")

  val dataset_jsonArray: Dataset[String] = sparkSession.read
    .textFile("src/test/resources/quickstart/incoming/hr/sellers-2018-01-01.json")

  val dataset_jsonArrayMultiline: Dataset[String] = sparkSession.read
    .textFile("src/test/resources/sample/simple-json-locations/locations.json")

  "GetSeparatorSemiColon" should "succeed" in {

    InferSchemaJob.getSeparator(dataset_csv) shouldBe ";"

  }

  "GetSeparatorPipe" should "succeed" in {

    InferSchemaJob.getSeparator(dataset_psv) shouldBe "|"

  }

  "GetFormatCSV" should "succeed" in {

    InferSchemaJob.getFormatFile(dataset_csv) shouldBe "DSV"

  }


  "GetFormatJson" should "succeed" in {

    InferSchemaJob.getFormatFile(dataset_json) shouldBe "JSON"

  }

  "GetFormatArrayJson" should "succeed" in {

    InferSchemaJob.getFormatFile(dataset_jsonArray) shouldBe "ARRAY_JSON"

  }

  "GetFormatArrayJsonMultiline" should "succeed" in {

    InferSchemaJob.getFormatFile(dataset_jsonArrayMultiline) shouldBe "ARRAY_JSON"

  }


}
