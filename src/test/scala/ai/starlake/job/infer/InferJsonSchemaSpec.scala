package ai.starlake.job.infer

import ai.starlake.TestHelper
import ai.starlake.schema.model.WriteMode
import better.files.File

class InferJsonSchemaSpec extends TestHelper {
  new WithSettings() {
    "InferSchema" should "infer schema from JSON Schema" in {
      val jsonSchemaContent =
        File(getClass.getResource("/sample-json-schema/sample_schema.json").toURI).contentAsString

      val inputPath = File.newTemporaryFile("schema", ".json").write(jsonSchemaContent)
      val outputPath = File.newTemporaryDirectory("output")

      val result = new InferSchemaJob().infer(
        domainName = "testIndex",
        tableName = "testTable",
        pattern = None,
        comment = None,
        inputPath = inputPath.pathAsString,
        saveDir = outputPath.pathAsString,
        forceFormat = None,
        writeMode = WriteMode.OVERWRITE,
        rowTag = None,
        clean = true,
        encoding = java.nio.charset.StandardCharsets.UTF_8,
        variant = false,
        fromJsonSchema = true
      )(settings.storageHandler())

      println("Infer Job Result: " + result)
      if (result.isFailure) {
        result.failed.get.printStackTrace()
      }
      result.isSuccess shouldBe true

      val resultFile =
        outputPath / "testIndex" / "testTable.sl.yml" // domain name is lowercased usually?
      // Wait, domainName passed is "testIndex".
      // Starlake usually handles case sensitivity. Let's check output.

      // File(outputPath.pathAsString).list.foreach(println)

      val loadFile = resultFile
      val content = loadFile.contentAsString
      println(content)

      content should include("name: \"testTable\"")
      content should include("attributes:")
      content should include("- name: \"firstName\"")
      content should include("type: \"string\"")
      content should include("required: true")
      content should include("- name: \"age\"")
      content should include("type: \"long\"")
      // content should include ("required: false") // age is not in required list
      content should include("- name: \"address\"")
      content should include("type: \"struct\"")
      content should include("attributes:")
      content should include("- name: \"street\"")

    }

    "InferSchema" should "infer schema from JSON Schema with $defs" in {
      val jsonSchemaContent =
        File(
          getClass.getResource("/sample-json-schema/sample_schema_defs.json").toURI
        ).contentAsString

      val inputPath = File.newTemporaryFile("schema_defs", ".json").write(jsonSchemaContent)
      val outputPath = File.newTemporaryDirectory("output_defs")

      val result = new InferSchemaJob().infer(
        domainName = "testIndexDefs",
        tableName = "testTableDefs",
        pattern = None,
        comment = None,
        inputPath = inputPath.pathAsString,
        saveDir = outputPath.pathAsString,
        forceFormat = None,
        writeMode = WriteMode.OVERWRITE,
        rowTag = None,
        clean = true,
        encoding = java.nio.charset.StandardCharsets.UTF_8,
        variant = false,
        fromJsonSchema = true
      )(settings.storageHandler())

      println("Infer Job Result: " + result)
      if (result.isFailure) {
        result.failed.get.printStackTrace()
      }
      result.isSuccess shouldBe true

      val resultFile =
        outputPath / "testIndexDefs" / "testTableDefs.sl.yml"

      val loadFile = resultFile
      val content = loadFile.contentAsString
      println(content)

      content should include("name: \"testTableDefs\"")
      content should include("- name: \"billingAddress\"")
      content should include("type: \"struct\"")
      content should include("- name: \"shippingAddress\"")
      content should include("type: \"struct\"")
      content should include("- name: \"street\"")
      content should include("type: \"string\"")
    }
  }
}
