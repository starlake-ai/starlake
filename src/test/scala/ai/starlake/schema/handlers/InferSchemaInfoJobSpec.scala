package ai.starlake.schema.handlers

import ai.starlake.TestHelper
import ai.starlake.job.infer.InferSchemaJob
import ai.starlake.schema.model.{Attribute, WriteMode}
import ai.starlake.utils.{Utils, YamlSerde}
import better.files.File
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import scala.io.Source

class InferSchemaInfoJobSpec extends TestHelper {

  new WithSettings() {

    implicit val storageHandlerImpl: StorageHandler = settings.storageHandler()

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
      inferSchemaJob.getSeparator(
        csvLines.head
      ) shouldBe ";"
    }

    "GetSeparatorPipe" should "succeed" in {
      inferSchemaJob.getSeparator(psvLines.head) shouldBe "|"
    }

    "GetFormatCSV" should "succeed" in {
      inferSchemaJob.getFormatFile(
        "src/test/resources/sample/SCHEMA-VALID-NOHEADER.dsv",
        StandardCharsets.UTF_8
      ) shouldBe "DSV"
    }

    "GetFormatJson" should "succeed" in {
      inferSchemaJob.getFormatFile(
        "src/test/resources/sample/json/complex.json",
        StandardCharsets.UTF_8
      ) shouldBe "JSON"
    }

    "GetFormatXML" should "succeed" in {
      inferSchemaJob.getFormatFile(
        "src/test/resources/sample/xml/locations.xml",
        StandardCharsets.UTF_8
      ) shouldBe "XML"
    }

    "GetFormatArrayJson" should "succeed" in {
      inferSchemaJob.getFormatFile(
        "src/test/resources/quickstart/incoming/hr/sellers-2018-01-01.json",
        StandardCharsets.UTF_8
      ) shouldBe "JSON_ARRAY"
    }

    "GetFormatArrayJsonMultiline" should "succeed" in {
      inferSchemaJob.getFormatFile(
        "src/test/resources/sample/simple-json-locations/locations.json",
        StandardCharsets.UTF_8
      ) shouldBe "JSON_ARRAY"
    }
    "Ingest Flat Locations JSON" should "produce file in accepted" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/flat-locations.json"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/simple-json-locations/locations.sl.yml",
          "/sample/simple-json-locations/flat_locations.sl.yml"
        ).foreach(deliverSourceTable)
        val inputData = loadTextFile("/sample/simple-json-locations/flat-locations.json")
        for {
          sourceFile <- File.temporaryFile()
          targetDir  <- File.temporaryDirectory()
        } {
          sourceFile.overwrite(inputData)
          val resultPath =
            inferSchemaJob.infer(
              domainName = "locations",
              tableName = "flat_locations",
              pattern = None,
              comment = None,
              inputPath = sourceFile.pathAsString,
              saveDir = targetDir.pathAsString,
              forceFormat = None,
              writeMode = WriteMode.OVERWRITE,
              rowTag = None,
              encoding = StandardCharsets.UTF_8,
              clean = false
            )(settings.storageHandler())
          val locationDir = File(targetDir, "locations")
          val targetConfig = File(locationDir, "_config.sl.yml")
          val maybeDomain =
            YamlSerde.deserializeYamlLoadConfig(
              targetConfig.contentAsString,
              targetConfig.pathAsString,
              isForExtract = false
            )
          maybeDomain.isSuccess shouldBe true
          val targetFile = File(locationDir, "flat_locations.sl.yml")
          val maybeTable = YamlSerde.deserializeYamlTables(
            targetFile.contentAsString,
            targetFile.pathAsString
          )

          val discoveredSchema = maybeTable.head.table
          discoveredSchema.name shouldBe "flat_locations"
          discoveredSchema.attributes.map(_.name) should contain theSameElementsAs List(
            "id",
            "name"
          )
        }
      }
    }

    "Ingest Complex JSON Schema" should "produce file in accepted" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/flat-locations.json"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/simple-json-locations/locations.sl.yml",
          "/sample/simple-json-locations/flat_locations.sl.yml"
        ).foreach(deliverSourceTable)
        val inputData = loadTextFile("/sample/complex-json/complex.json")
        for {
          sourceFile <- File.temporaryFile()
          targetDir  <- File.temporaryDirectory()
        } {
          sourceFile.overwrite(inputData)
          inferSchemaJob.infer(
            domainName = "locations",
            tableName = "complex",
            pattern = None,
            comment = None,
            inputPath = sourceFile.pathAsString,
            saveDir = targetDir.pathAsString,
            forceFormat = None,
            writeMode = WriteMode.OVERWRITE,
            rowTag = None,
            encoding = StandardCharsets.UTF_8,
            clean = false
          )(settings.storageHandler())
          val locationDir = File(targetDir, "locations")
          val targetFile = File(locationDir, "complex.sl.yml")
          val discoveredSchema = YamlSerde.deserializeYamlTables(
            targetFile.contentAsString,
            targetFile.pathAsString
          )

          val expectedTable = YamlSerde.deserializeYamlTables(
            loadTextFile("/sample/complex-json/complex.sl.yml"),
            "/sample/complex-json/complex.sl.yml"
          )

          // restrit to name & type because sample
          removeSampleField(
            discoveredSchema.head.table.attributes
          ) should contain theSameElementsAs removeSampleField(expectedTable.head.table.attributes)
        }
      }
    }

    private def removeSampleField(attributes: List[Attribute]): List[Attribute] = {
      attributes.map { attr =>
        if (attr.attributes.nonEmpty) {
          attr.copy(attributes = removeSampleField(attr.attributes), sample = None)
        } else {
          attr.copy(sample = None)
        }
      }

    }
    "Ingest Complete CSV Schema" should "produce file in accepted" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/flat-locations.json"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/simple-json-locations/locations.sl.yml",
          "/sample/simple-json-locations/flat_locations.sl.yml"
        ).foreach(deliverSourceTable)
        val inputData = loadTextFile("/sample/complete-csv/complete.csv")
        for {
          sourceFile <- File.temporaryFile()
          targetDir  <- File.temporaryDirectory()
        } {
          sourceFile.overwrite(inputData)
          inferSchemaJob.infer(
            domainName = "locations",
            tableName = "complete",
            pattern = None,
            comment = None,
            inputPath = sourceFile.pathAsString,
            saveDir = targetDir.pathAsString,
            forceFormat = None,
            writeMode = WriteMode.OVERWRITE,
            rowTag = None,
            encoding = StandardCharsets.UTF_8,
            clean = false
          )(settings.storageHandler())
          val locationDir = File(targetDir, "locations")
          val targetFile = File(locationDir, "complete.sl.yml")
          val discoveredSchema = YamlSerde.deserializeYamlTables(
            targetFile.contentAsString,
            targetFile.pathAsString
          )

          val expectedTable = YamlSerde.deserializeYamlTables(
            loadTextFile("/sample/complete-csv/complete.sl.yml"),
            "/sample/complete-csv/complete.sl.yml"
          )

          discoveredSchema.head.table.attributes should contain theSameElementsAs expectedTable.head.table.attributes
        }
      }
    }

    "Ingest JSON Schema" should "produce file in accepted" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/flat-locations.json"
      ) {
        cleanMetadata
        val inputData0 = loadTextFile("/sample/infer-schema/jsonarraysimple.json")
        for {
          sourceFile <- File.temporaryFile()
          targetDir  <- File.temporaryDirectory()
        } {
          sourceFile.overwrite(inputData0)
          inferSchemaJob.infer(
            domainName = "json",
            tableName = "jsonarraysimple",
            pattern = None,
            comment = None,
            inputPath = sourceFile.pathAsString,
            saveDir = targetDir.pathAsString,
            forceFormat = None,
            writeMode = WriteMode.OVERWRITE,
            rowTag = None,
            encoding = StandardCharsets.UTF_8,
            clean = false
          )(settings.storageHandler())
          val locationDir = File(targetDir, "json")
          val targetFile = File(locationDir, "jsonarraysimple.sl.yml")
          val discoveredSchema = YamlSerde.deserializeYamlTables(
            targetFile.contentAsString,
            targetFile.pathAsString
          )
          println(YamlSerde.mapper.writeValueAsString(discoveredSchema))
        }

        val inputData = loadTextFile("/sample/infer-schema/jsonarray.json")
        for {
          sourceFile <- File.temporaryFile()
          targetDir  <- File.temporaryDirectory()
        } {
          sourceFile.overwrite(inputData)
          inferSchemaJob.infer(
            domainName = "json",
            tableName = "array",
            pattern = None,
            comment = None,
            inputPath = sourceFile.pathAsString,
            saveDir = targetDir.pathAsString,
            forceFormat = None,
            writeMode = WriteMode.OVERWRITE,
            rowTag = None,
            encoding = StandardCharsets.UTF_8,
            clean = false
          )(settings.storageHandler())
          val locationDir = File(targetDir, "json")
          val targetFile = File(locationDir, "array.sl.yml")
          val discoveredSchema = YamlSerde.deserializeYamlTables(
            targetFile.contentAsString,
            targetFile.pathAsString
          )
          println(YamlSerde.mapper.writeValueAsString(discoveredSchema))
        }

        val inputData2 = loadTextFile("/sample/infer-schema/ndjson.json")
        for {
          sourceFile <- File.temporaryFile()
          targetDir  <- File.temporaryDirectory()
        } {
          sourceFile.overwrite(inputData2)
          inferSchemaJob.infer(
            domainName = "json",
            tableName = "ndjson",
            pattern = None,
            comment = None,
            inputPath = sourceFile.pathAsString,
            saveDir = targetDir.pathAsString,
            forceFormat = None,
            writeMode = WriteMode.OVERWRITE,
            rowTag = None,
            encoding = StandardCharsets.UTF_8,
            clean = false
          )(settings.storageHandler())
          val locationDir = File(targetDir, "json")
          val targetFile = File(locationDir, "ndjson.sl.yml")
          val discoveredSchema = YamlSerde.deserializeYamlTables(
            targetFile.contentAsString,
            targetFile.pathAsString
          )
          println(YamlSerde.mapper.writeValueAsString(discoveredSchema))
        }

        val inputData3 = loadTextFile("/sample/infer-schema/variant.json")
        for {
          sourceFile <- File.temporaryFile()
          targetDir  <- File.temporaryDirectory()
        } {
          sourceFile.overwrite(inputData3)
          inferSchemaJob.infer(
            domainName = "json",
            tableName = "variantjson",
            pattern = None,
            comment = None,
            inputPath = sourceFile.pathAsString,
            saveDir = targetDir.pathAsString,
            forceFormat = None,
            writeMode = WriteMode.OVERWRITE,
            rowTag = None,
            encoding = StandardCharsets.UTF_8,
            clean = false
          )(settings.storageHandler())
          val locationDir = File(targetDir, "json")
          val targetFile = File(locationDir, "variantjson.sl.yml")
          val discoveredSchema = YamlSerde.deserializeYamlTables(
            targetFile.contentAsString,
            targetFile.pathAsString
          )
          println(YamlSerde.mapper.writeValueAsString(discoveredSchema))
        }

      }
    }
    "Infer parquet Schema" should "produce file in accepted" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/infer-schema/userdata1.parquet"
      ) {
        cleanMetadata
        for {
          sourceFile <- File.temporaryFile()
          targetDir  <- File.temporaryDirectory()
        } {
          val targetPath = new Path(tempDir.toString, "userdata1.parquet")
          withSettings.deliverBinaryFile(sourceDatasetPathName, targetPath)
          inferSchemaJob.infer(
            domainName = "parquet",
            tableName = "userdata1",
            pattern = None,
            comment = None,
            inputPath = targetPath.toString,
            saveDir = targetDir.pathAsString,
            forceFormat = None,
            writeMode = WriteMode.OVERWRITE,
            rowTag = None,
            encoding = StandardCharsets.UTF_8,
            clean = false
          )(settings.storageHandler())
          val locationDir = File(targetDir, "parquet")
          val targetFile = File(locationDir, "userdata1.sl.yml")
          val discoveredSchema = YamlSerde.deserializeYamlTables(
            targetFile.contentAsString,
            targetFile.pathAsString
          )
          println(YamlSerde.mapper.writeValueAsString(discoveredSchema))
        }
      }
    }
  }

}
