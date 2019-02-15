package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.{TestHelper, TypeToImport}
import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.workflow.DatasetWorkflow
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

import scala.util.Try

class SchemaHandlerSpec extends TestHelper {

  // TODO Helper (to delete)
  def printDF(df: DataFrame) = {
    df.printSchema
    df.show(false)
  }

  "Ingest CSV" should "produce file in accepted" in {

    new SpecTrait {
      override val domain: Path = DatasetArea.domains
      override val domainName: String = "DOMAIN.yml"
      override val domainFile: String = "/sample/DOMAIN.yml"

      override val types: List[TypeToImport] = List(
        TypeToImport(
          "types.yml",
          "/sample/types.yml"
        )
      )

      override val targetName: String = "DOMAIN"
      override val targetFile: String = "/sample/SCHEMA-VALID.dsv"

      launch

      // Check Archived
      readFileContent(cometDatasetsPath + s"/archive/$targetName/SCHEMA-VALID.dsv") shouldBe loadFile(
        targetFile
      )

      // Check rejected

      val rejectedDf = sparkSession.read
        .parquet(cometDatasetsPath + s"/rejected/$targetName/User")

      val expectedRejectedF = prepareDateColumns(
        sparkSession.read
          .schema(prepareSchema(rejectedDf.schema))
          .json(getResPath("/expected/datasets/rejected/DOMAIN.json"))
      )

      expectedRejectedF.except(rejectedDf).count() shouldBe 0

      // Accepted should have the same data as input
      val acceptedDf = sparkSession.read
        .parquet(cometDatasetsPath + s"/accepted/$targetName/User/${getTodayPartitionPath}")

      printDF(acceptedDf)
      val expectedAccepted = (
        sparkSession.read
          .schema(acceptedDf.schema)
          .json(getResPath("/expected/datasets/accepted/DOMAIN/User.json"))
        )

      printDF(expectedAccepted)
      acceptedDf.except(expectedAccepted).count() shouldBe 0

    }

  }

  "Ingest Dream Contact CSV" should "produce file in accepted" in {

    new SpecTrait {
      override val domain: Path = DatasetArea.domains
      override val domainName: String = "dream.yml"
      override val domainFile: String = "/sample/dream/dream.yml"

      override val types: List[TypeToImport] = List(
        TypeToImport(
          "default.yml",
          "/sample/default.yml"
        ),
        TypeToImport(
          "types.yml",
          "/sample/dream/types.yml"
        )
      )

      override val targetName: String = "dream"
      override val targetFile: String = "/sample/dream/OneClient_Contact_20190101_090800_008.psv"

      launch

      readFileContent(
        cometDatasetsPath + s"/archive/$targetName/OneClient_Contact_20190101_090800_008.psv"
      ) shouldBe loadFile(
        targetFile
      )

      //If we run this test alone, we do not have rejected, else we have rejected but not accepted ...
      Try {
        printDF(
          sparkSession.read.parquet(
            cometDatasetsPath + "/rejected/dream/client"
          )
        )
      }

      // Accepted should have the same data as input
      val acceptedDf = sparkSession.read
        .parquet(cometDatasetsPath + s"/accepted/$targetName/client/${getTodayPartitionPath}")

      val expectedAccepted = (
        sparkSession.read
          .schema(acceptedDf.schema)
          .json(getResPath("/expected/datasets/accepted/dream/client.json"))
        )

      acceptedDf.except(expectedAccepted).count() shouldBe 0

    }

  }

  "Ingest Dream Segment CSV" should "produce file in accepted" in {

    new SpecTrait {
      override val domain: Path = DatasetArea.domains
      override val domainName: String = "dream.yml"
      override val domainFile: String = "/sample/dream/dream.yml"

      override val types: List[TypeToImport] = List(
        TypeToImport(
          "default.yml",
          "/sample/default.yml"
        ),
        TypeToImport(
          "dream.yml",
          "/sample/dream/types.yml"
        )
      )

      override val targetName: String = "dream"
      override val targetFile: String =
        "/sample/dream/OneClient_Segmentation_20190101_090800_008.psv"

      launch

      readFileContent(
        cometDatasetsPath + s"/archive/$targetName/OneClient_Segmentation_20190101_090800_008.psv"
      ) shouldBe loadFile(
        targetFile
      )

      // Accepted should have the same data as input
      val acceptedDf = sparkSession.read
        .parquet(cometDatasetsPath + s"/accepted/$targetName/segment/${getTodayPartitionPath}")

      val expectedAccepted = (
        sparkSession.read
          .schema(acceptedDf.schema)
          .json(getResPath("/expected/datasets/accepted/dream/segment.json"))
        )
      acceptedDf.except(expectedAccepted).count() shouldBe 0

    }

  }

  "Ingest Dream Locations JSON" should "produce file in accepted" in {

    new SpecTrait {
      override val domain: Path = DatasetArea.domains
      override val domainName: String = "locations.yml"
      override val domainFile: String = "/sample/simple-json-locations/locations.yml"

      override val types: List[TypeToImport] = List(
        TypeToImport(
          "types.yml",
          "/sample/simple-json-locations/types.yml"
        )
      )

      override val targetName: String = "locations"
      override val targetFile: String =
        "/sample/simple-json-locations/locations.json"

      launch

      readFileContent(
        cometDatasetsPath + s"/archive/$targetName/locations.json"
      ) shouldBe loadFile(
        targetFile
      )

      // Accepted should have the same data as input
      val acceptedDf = sparkSession.read
        .parquet(cometDatasetsPath + s"/accepted/$targetName/locations/${getTodayPartitionPath}")

      val expectedAccepted = (
        sparkSession.read
          .json(
            getResPath("/expected/datasets/accepted/locations/locations.json")
          )
        )

      acceptedDf.except(expectedAccepted).count() shouldBe 0

    }

  }
  //TODO TOFIX
//  "Load Business Definition" should "produce business dataset" in {
//    val sh = new HdfsStorageHandler
//    val jobsPath = new Path(DatasetArea.jobs, "sample/metadata/business/business.yml")
//    sh.write(loadFile("/sample/metadata/business/business.yml"), jobsPath)
//    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))
//    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
//    validator.autoJob("business1")
//  }

  "Writing types" should "work" in {

    val typesPath = new Path(DatasetArea.types, "types.yml")

    storageHandler.write(loadFile("/sample/types.yml"), typesPath)

    readFileContent(typesPath) shouldBe loadFile("/sample/types.yml")

  }

}
