package com.ebiznext.comet.job

import java.sql.Timestamp
import java.time.Instant

import com.ebiznext.comet.config.{DatasetArea, HiveArea}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.json.JsonUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class JsonIngestionJob(val domain: Domain, val schema: Schema, val types: List[Type], val path: Path, storageHandler: StorageHandler) extends IngestionJob {

  def loadDataSet(): DataFrame = {
    val df = session.read.format("com.databricks.spark.csv")
      .option("inferSchema", value = false)
      .text(path.toString)
    df.show()
    df
  }

  lazy val schemaSparkType: StructType = schema.sparkType(Types(types))


  def ingest(dataset: DataFrame): Unit = {
    val rdd = dataset.rdd
    dataset.show()
    val checkedRDD = JsonUtil.parseRDD(rdd, schemaSparkType).cache()
    val acceptedRDD: RDD[String] = checkedRDD.filter(_.isRight).map(_.right.get)
    val rejectedRDD: RDD[String] = checkedRDD.filter(_.isLeft).map(_.left.get.mkString("\n"))
    rejectedRDD.collect().foreach(println)
    val acceptedDF = session.read.json(acceptedRDD)
    saveRejected(rejectedRDD)
    saveAccepted(acceptedDF)
  }

  def saveAccepted(acceptedDF: DataFrame): Unit = {
    val writeMode = metadata.getWrite()
    val acceptedPath = new Path(DatasetArea.accepted(domain.name), schema.name)
    saveRows(acceptedDF, acceptedPath, writeMode, HiveArea.accepted)
  }

  def saveAccepted(acceptedRDD: RDD[Row]): Unit = {
    val writeMode = metadata.getWrite()
    val acceptedPath = new Path(DatasetArea.accepted(domain.name), schema.name)
    saveRows(session.createDataFrame(acceptedRDD, schemaSparkType), acceptedPath, writeMode, HiveArea.accepted)
  }


  override def name: String = "JsonJob"
}