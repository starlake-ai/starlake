package com.ebiznext.comet.job

import com.ebiznext.comet.config.{DatasetArea, HiveArea}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.json.JsonIngestionUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}


/**
  * Main class to complex json delimiter separated values file
  * If your json contains only one level simple attribute aka. kind of dsv but in json format please use SIMPLE_JSON instead. It's way faster
  *
  * @param domain         : Input Dataset Domain
  * @param schema         : Input Dataset Schema
  * @param types          : List of globally defined types
  * @param path           : Input dataset path
  * @param storageHandler : Storage Handler
  */
class JsonIngestionJob(val domain: Domain, val schema: Schema, val types: List[Type], val path: Path, val storageHandler: StorageHandler) extends IngestionJob {

  /**
    * load the json as an RDD of String
    * @return Spark Dataframe loaded using metadata options
    */
  def loadDataSet(): DataFrame = {
    val df = session.read.format("com.databricks.spark.csv")
      .option("inferSchema", value = false)
      .text(path.toString)
    df.printSchema()
    df
  }

  lazy val schemaSparkType: StructType = schema.sparkType(Types(types))


  /**
    * Where the magic happen
    * @param dataset input dataset as a RDD of string
    */
  def ingest(dataset: DataFrame): Unit = {
    val rdd = dataset.rdd
    dataset.printSchema()
    val checkedRDD = JsonIngestionUtil.parseRDD(rdd, schemaSparkType).cache()
    val acceptedRDD: RDD[String] = checkedRDD.filter(_.isRight).map(_.right.get)
    val rejectedRDD: RDD[String] = checkedRDD.filter(_.isLeft).map(_.left.get.mkString("\n"))
    rejectedRDD.collect().foreach(println)
    val acceptedDF = session.read.json(acceptedRDD)
    saveRejected(rejectedRDD)
    saveAccepted(acceptedDF) // prefer to let Spark compute the final schema
  }

  /**
    * Use the schema we used for validation when saving
    * @param acceptedRDD
    */
  @deprecated("We let Spark compute the final schema")
  def saveAccepted(acceptedRDD: RDD[Row]): Unit = {
    val writeMode = metadata.getWriteMode()
    val acceptedPath = new Path(DatasetArea.accepted(domain.name), schema.name)
    saveRows(session.createDataFrame(acceptedRDD, schemaSparkType), acceptedPath, writeMode, HiveArea.accepted)
  }


  override def name: String = "JsonJob"
}