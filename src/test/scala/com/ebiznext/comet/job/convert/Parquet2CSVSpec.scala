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
    "CreateAttributes" should "create the correct list of attributes for a complex Json" in {
      val rootDir = File.newTemporaryDirectory()
      val outputDir = File(rootDir, "output")
      val domainName = "domain"
      val domainPath = File(rootDir, domainName).createDirectory()
      val schemaName = "schema"
      val schemaPath = File(domainPath, schemaName)
      val data = List(
        Schema("12345", "A009701", 123.65, "AQZERD"),
        Schema("56432", "A000000", 23.8, "AQZERD"),
        Schema("12345", "A009701", 123.6, "AQZERD")
      )
      val dataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data))
      dataFrame.write.mode(SaveMode.ErrorIfExists).parquet(schemaPath.pathAsString)
      val config = Parquet2CSVConfig(
        new Path(rootDir.pathAsString),
        Some(new Path(outputDir.pathAsString)),
        Some(domainName),
        Some(schemaName),
        true,
        Some(WriteMode.ERROR_IF_EXISTS),
        "|",
        1
      )
      new Parquet2CSV(config, storageHandler).run()
      val csvFile = File(outputDir, domainName, schemaName + ".csv")
      val result = Source.fromFile(csvFile.pathAsString).getLines().toList
      val expected = List(
        "id|customer|amount|seller_id",
        "12345|A009701|123.6|AQZERD",
        "56432|A000000|23.8|AQZERD",
        "12345|A009701|123.65|AQZERD"
      )
      rootDir.delete()
      expected should contain theSameElementsAs result
    }
  }
}
