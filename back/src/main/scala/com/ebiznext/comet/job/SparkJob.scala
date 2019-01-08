package com.ebiznext.comet.job

import com.ebiznext.comet.config.SparkEnv
import com.ebiznext.comet.schema.model.Metadata
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

trait SparkJob extends StrictLogging {
  def name: String

  lazy val sparkEnv = new SparkEnv(name)
  lazy val session: SparkSession = sparkEnv.session

  def run(args: Array[String]): SparkSession

  def partitionedDatasetWriter(dataset: DataFrame, partition: List[String]): DataFrameWriter[Row] = {
    partition match {
      case Nil => dataset.write
      case cols if cols.forall(Metadata.CometPartitionColumns.contains) =>
        // TODO Should we issue a warning if used with Overwrite mode ????
        var partitionedDF = dataset.withColumn("comet_date", current_date())
        cols.foreach {
          case "comet_year" => partitionedDF = partitionedDF.withColumn("year", year(col("comet_date")))
          case "comet_month" => partitionedDF = partitionedDF.withColumn("month", month(col("comet_date")))
          case "comet_day" => partitionedDF = partitionedDF.withColumn("day", dayofmonth(col("comet_date")))
          case "comet_hour" => partitionedDF = partitionedDF.withColumn("hour", hour(col("comet_date")))
          case "comet_minute" => partitionedDF = partitionedDF.withColumn("minute", minute(col("comet_date")))
        }
        val strippedCols = cols.map(_.substring("comet_".length))
        partitionedDF.drop("comet_date").write.partitionBy(strippedCols: _*)
      case cols if !cols.exists(Metadata.CometPartitionColumns.contains) =>
        dataset.write.partitionBy(cols: _*)
    }
  }
}
