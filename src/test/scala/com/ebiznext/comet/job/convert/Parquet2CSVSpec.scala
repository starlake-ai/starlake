package com.ebiznext.comet.job.convert

import better.files.File
import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.schema.model.WriteMode
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode

import scala.io.Source

case class Schema(id: String, customer: String, amount: Double, seller_id: String)

class Parquet2CSVSpec extends TestHelper {
  new WithSettings() {

    val rootDir = File.newTemporaryDirectory()
    val outputDir = File(rootDir, "output")
    val domainName = "domain"
    val domainPath = File(rootDir, domainName).createDirectory()
    val schemaName = "schema"
    val schemaPath = File(domainPath, schemaName)

    val config = Parquet2CSVConfig(
      new Path(rootDir.pathAsString),
      Some(new Path(outputDir.pathAsString)),
      Some(domainName),
      Some(schemaName),
      true,
      Some(WriteMode.ERROR_IF_EXISTS),
      deleteSource = false,
      "|",
      1
    )

    def createParquet() = {
      val data = List(
        Schema("12345", "A009701", 123.65, "AQZERD"),
        Schema("56432", "A000000", 23.8, "AQZERD"),
        Schema("12345", "A009701", 123.6, "AQZERD")
      )
      val dataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data))
      dataFrame.write.mode(SaveMode.ErrorIfExists).parquet(schemaPath.pathAsString)
    }

    "Convert schema" should "succeed" in {
      createParquet()
      new Parquet2CSV(config, storageHandler).run()
      val csvFile = File(outputDir.pathAsString, domainName, schemaName + ".csv")
      val result = Source.fromFile(csvFile.pathAsString).getLines().toList
      val expected = List(
        "id|customer|amount|seller_id",
        "12345|A009701|123.6|AQZERD",
        "56432|A000000|23.8|AQZERD",
        "12345|A009701|123.65|AQZERD"
      )
      csvFile.delete()
      rootDir.delete()
      expected should contain theSameElementsAs result
    }
    "Convert schema" should "succeed delete source after completion" in {
      createParquet()
      val inputPath = File(rootDir.pathAsString, domainName, schemaName)
      inputPath.exists shouldBe true
      new Parquet2CSV(config.copy(deleteSource = true), storageHandler).run()
      inputPath.exists shouldBe false
      rootDir.delete()
    }
    "Convert schema" should "convert all schemas in domain" in {
      createParquet()
      val inputPath = File(rootDir.pathAsString, domainName, schemaName)
      inputPath.exists shouldBe true
      val csvFile = File(outputDir.pathAsString, domainName, schemaName + ".csv")
      csvFile.exists shouldBe false
      new Parquet2CSV(config.copy(schemaName = None), storageHandler).run()
      csvFile.delete()
      rootDir.delete()
    }
    "Convert schema" should "convert all domains and schemas in path" in {
      createParquet()
      val inputPath = File(rootDir.pathAsString, domainName, schemaName)
      inputPath.exists shouldBe true
      val csvFile = File(outputDir.pathAsString, domainName, schemaName + ".csv")
      csvFile.exists shouldBe false
      new Parquet2CSV(config, storageHandler).run()
      csvFile.exists shouldBe true
      csvFile.delete()
      rootDir.delete()
    }
  }
}
