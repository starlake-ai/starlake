package com.ebiznext.comet.utils

import com.ebiznext.comet.config.{Settings, SparkEnv, UdfRegistration}
import com.ebiznext.comet.schema.model.Metadata
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

import scala.util.Try

case class SparkJobResult(session: SparkSession, df: Option[DataFrame] = None)

/**
  * All Spark Job extend this trait.
  * Build Spark session using spark variables from applciation.conf.
  */
trait SparkJob extends StrictLogging {
  def name: String
  implicit def settings: Settings

  lazy val sparkEnv: SparkEnv = new SparkEnv(name)

  lazy val session: SparkSession = {
    val udfs = settings.comet.udfs.map { udfs =>
        udfs.split(',').toList
      } getOrElse Nil

    udfs.foreach { udf =>
      val udfInstance: UdfRegistration =
        Class
          .forName(udf)
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[UdfRegistration]
      udfInstance.register(sparkEnv.session)
    }
    sparkEnv.session
  }

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  def run(): Try[SparkJobResult]

  // TODO Should we issue a warning if used with Overwrite mode ????
  // TODO Check that the year / month / day / hour / minute do not already exist
  private def buildPartitionedDF(dataset: DataFrame, cols: List[String]): DataFrame = {
    var partitionedDF = dataset.withColumn("comet_date", current_timestamp())
    val dataSetsCols = dataset.columns.toList
    cols.foreach {
      case "comet_year" if !dataSetsCols.contains("year") =>
        partitionedDF = partitionedDF.withColumn("year", year(col("comet_date")))
      case "comet_month" if !dataSetsCols.contains("month") =>
        partitionedDF = partitionedDF.withColumn("month", month(col("comet_date")))
      case "comet_day" if !dataSetsCols.contains("day") =>
        partitionedDF = partitionedDF.withColumn("day", dayofmonth(col("comet_date")))
      case "comet_hour" if !dataSetsCols.contains("hour") =>
        partitionedDF = partitionedDF.withColumn("hour", hour(col("comet_date")))
      case "comet_minute" if !dataSetsCols.contains("minute") =>
        partitionedDF = partitionedDF.withColumn("minute", minute(col("comet_date")))
      case _ =>
        partitionedDF
    }
    partitionedDF.drop("comet_date")
  }

  /**
    * Partition a dataset using dataset columns.
    * To partition the dataset using the igestion time, use the reserved column names :
    *   - comet_year
    *   - comet_month
    *   - comet_day
    *   - comet_hour
    *   - comet_minute
    * These columsn are renamed to "year", "month", "day", "hour", "minute" in the dataset and
    * their values is set to the current date/time.
    *
    * @param dataset   : Input dataset
    * @param partition : list of columns to use for partitioning.
    * @return The Spark session used to run this job
    */
  def partitionedDatasetWriter(
    dataset: DataFrame,
    partition: List[String]
  ): DataFrameWriter[Row] = {
    partition match {
      case Nil => dataset.write
      case cols if cols.forall(Metadata.CometPartitionColumns.contains) =>
        val strippedCols = cols.map(_.substring("comet_".length))
        val partitionedDF = buildPartitionedDF(dataset, cols)
        // does not work on nested fields -> https://issues.apache.org/jira/browse/SPARK-18084
        partitionedDF.write.partitionBy(strippedCols: _*)
      case cols if !cols.exists(Metadata.CometPartitionColumns.contains) =>
        dataset.write.partitionBy(cols: _*)
      case _ =>
        // Should never happend
        // TODO Test this at load time
        throw new Exception("Cannot mix comet & non comet col names")

    }
  }

  def partitionDataset(dataset: DataFrame, partition: List[String]): DataFrame = {
    logger.info(s"""Partitioning on ${partition.mkString(",")}""")
    partition match {
      case Nil => dataset
      case cols if cols.forall(Metadata.CometPartitionColumns.contains) =>
        buildPartitionedDF(dataset, cols)
      case cols if !cols.exists(Metadata.CometPartitionColumns.contains) =>
        dataset
      case _ =>
        dataset

    }
  }

}
