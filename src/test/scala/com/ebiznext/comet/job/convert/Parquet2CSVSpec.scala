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
      Some(WriteMode.ERROR_IF_EXISTS),
      deleteSource = false,
      options = List("sep" -> "|", "header" -> "true"),
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

    "All Parquet Config" should "be known and taken  into account" in {
      val rendered = Parquet2CSVConfig.usage()
      val expected =
        """
        |Usage: comet [options]
        |
        |example => --input_dir /tmp/datasets/accepted/ --output_dir /tmp/datasets/csv/ --domain sales --schema orders --option header=true  --option separator=,  --partitions 1 --write_mode overwrite
        |  --input_dir <value>   Full Path to input directory
        |  --output_dir <value>  Full Path to output directory, input_dir is used if not present
        |  --domain <value>      Domain Name
        |  --schema <value>      Schema Name
        |  --delete_source       delete source parquet file ?
        |  --write_mode <value>  One of Set(OVERWRITE, APPEND, ERROR_IF_EXISTS, IGNORE)
        |  --option <value>      option to use (sep, delimiter, quote, quoteAll, escape, header ...)
        |  --partitions <value>  How many output partitions
        |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")
    }
  }
}
