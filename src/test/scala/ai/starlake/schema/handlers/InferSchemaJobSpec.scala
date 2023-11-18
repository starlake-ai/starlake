package ai.starlake.schema.handlers

import ai.starlake.TestHelper
import ai.starlake.job.infer.InferSchemaJob
import ai.starlake.schema.model.WriteMode
import ai.starlake.utils.{Utils, YamlSerializer}
import better.files.File

import scala.io.Source

class InferSchemaJobSpec extends TestHelper {

  new WithSettings() {

    lazy val csvLines =
      Utils.withResources(Source.fromFile("src/test/resources/sample/SCHEMA-VALID-NOHEADER.dsv"))(
        _.getLines().toList
      )

    lazy val psvLines =
      Utils.withResources(
        Source.fromFile("src/test/resources/quickstart/incoming/sales/customers-2018-01-01.psv")
      )(
        _.getLines().toList
      )

    lazy val jsonLines =
      Utils.withResources(Source.fromFile("src/test/resources/sample/json/complex.json"))(
        _.getLines().toList
      )

    lazy val jsonArrayLines =
      Utils.withResources(
        Source.fromFile("src/test/resources/quickstart/incoming/hr/sellers-2018-01-01.json")
      )(
        _.getLines().toList
      )

    lazy val jsonArrayMultilinesLines =
      Utils.withResources(
        Source.fromFile("src/test/resources/sample/simple-json-locations/locations.json")
      )(
        _.getLines().toList
      )

    lazy val xmlLines =
      Utils.withResources(
        Source.fromFile("src/test/resources/sample/xml/locations.xml")
      )(
        _.getLines().toList
      )

    lazy val inferSchemaJob: InferSchemaJob = new InferSchemaJob()

    "GetSeparatorSemiColon" should "succeed" in {
      inferSchemaJob.getSeparator(csvLines, false) shouldBe ";"
    }

    "GetSeparatorPipe" should "succeed" in {
      inferSchemaJob.getSeparator(psvLines, true) shouldBe "|"
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
    "Ingest Flat Locations JSON" should "produce file in accepted" in {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/simple-json-locations/locations.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/flat-locations.json"
      ) {
        cleanMetadata
        cleanDatasets
        val inputData = loadTextFile("/sample/simple-json-locations/flat-locations.json")
        for {
          sourceFile <- File.temporaryFile()
          targetDir  <- File.temporaryDirectory()
        } {
          sourceFile.overwrite(inputData)
          inferSchemaJob.infer(
            domainName = "locations",
            schemaName = "flat_locations",
            pattern = None,
            comment = None,
            dataPath = sourceFile.pathAsString,
            saveDir = targetDir.pathAsString,
            withHeader = true,
            forceFormat = None,
            writeMode = WriteMode.OVERWRITE
          )
          val locationDir = File(targetDir, "locations")
          val targetConfig = File(locationDir, "_config.sl.yml")
          val maybeDomain =
            YamlSerializer.deserializeDomain(
              targetConfig.contentAsString,
              targetConfig.pathAsString
            )
          maybeDomain.isSuccess shouldBe true
          val targetFile = File(locationDir, "flat_locations.sl.yml")
          val maybeTable = YamlSerializer.deserializeSchema(
            targetFile.contentAsString,
            targetFile.pathAsString
          )

          val discoveredSchema = maybeTable.get
          discoveredSchema.name shouldBe "flat_locations"
          discoveredSchema.attributes.map(_.name) should contain theSameElementsAs List(
            "id",
            "name"
          )
        }
      }
    }
  }
}
