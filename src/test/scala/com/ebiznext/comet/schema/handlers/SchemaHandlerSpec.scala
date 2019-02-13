package com.ebiznext.comet.schema.handlers

import java.io.InputStream
import java.time.LocalDate

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.schema.model.Domain
import com.ebiznext.comet.workflow.DatasetWorkflow
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.{FlatSpec, Matchers}

case class TypeToImport(name: String, path: String)

class SchemaHandlerSpec extends TestHelper {

  def pp(df: DataFrame) = {
    df.printSchema
    df.show(false)
  }

  trait SpecTrait {
    val domain: Path
    val domainName: String
    val domainFile: String

    val types: List[TypeToImport]

    val targetName: String
    val targetFile: String

    def launch: Unit = {

      cleanMetadata

      val domainsPath = new Path(domain, domainName)

      storageHandler.write(loadFile(domainFile), domainsPath)

      types.foreach { typeToImport =>
        val typesPath = new Path(DatasetArea.types, typeToImport.name)
        storageHandler.write(loadFile(typeToImport.path), typesPath)
      }

      DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

      DatasetArea.init(storageHandler)

      val targetPath =
        DatasetArea.path(DatasetArea.pending(targetName), new Path(targetFile).getName)
      storageHandler.write(loadFile(targetFile), targetPath)

      val validator =
        new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)

      validator.loadPending()
    }

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

      pp(acceptedDf)
      val expectedAccepted = (
        sparkSession.read
          .schema(acceptedDf.schema)
          .json(getResPath("/expected/datasets/accepted/DOMAIN/User.json"))
        )

      pp(expectedAccepted)
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
          "dream.yml",
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
  //TODO TOFIX & shouldn't we test sth ?
//  "Load Business Definition" should "produce business dataset" in {
//    val sh = new HdfsStorageHandler
//    val jobsPath = new Path(DatasetArea.jobs, "sample/metadata/business/business.yml")
//    sh.write(loadFile("/sample/metadata/business/business.yml"), jobsPath)
//    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))
//    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
//    validator.autoJob("business1")
//  }
//  //TODO shouldn't we test sth ?
//  "Load Types" should "deserialize string as pattern" in {
//    val typesPath = new Path(DatasetArea.types, "types.yml")
//    val sh = new HdfsStorageHandler
//    sh.write(loadFile("/sample/types.yml"), typesPath)
////    assert(true)
//  }
//  //TODO TOFIX & shouldn't we test sth ?
////  "Import" should "copy to HDFS" in {
////    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
////    validator.loadLanding()
////  }
//  //TODO shouldn't we test sth ?
//  "Watch" should "request Airflow" in {
//    val validator =
//      new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
//    validator.loadPending()
//  }

}
