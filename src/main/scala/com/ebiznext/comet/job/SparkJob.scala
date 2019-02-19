/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.ebiznext.comet.job

import com.ebiznext.comet.config.SparkEnv
import com.ebiznext.comet.schema.model.Metadata
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

/**
  * All Spark Job extend this trait.
  * Build Spark session using spark variables from applciation.conf.
  */
trait SparkJob extends StrictLogging {
  def name: String

  lazy val sparkEnv: SparkEnv = new SparkEnv(name)
  lazy val session: SparkSession = sparkEnv.session

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @param args : arbitrary list of arguments
    * @return : Spark Session used for the job
    */
  def run(): SparkSession

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
      case Nil                                                          => dataset.write
      case cols if cols.forall(Metadata.CometPartitionColumns.contains) =>
        // TODO Should we issue a warning if used with Overwrite mode ????
        // TODO Check that the year / month / day / hour / minute do not already exist
        var partitionedDF = dataset.withColumn("comet_date", current_date())
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
        }
        val strippedCols = cols.map(_.substring("comet_".length))
        // does not work on nested fields -> https://issues.apache.org/jira/browse/SPARK-18084
        partitionedDF.drop("comet_date").write.partitionBy(strippedCols: _*)
      case cols if !cols.exists(Metadata.CometPartitionColumns.contains) =>
        dataset.write.partitionBy(cols: _*)
      case _ =>
        // Should never happend
        // TODO Test this at load time
        throw new Exception("Cannot mix comet & non comet col names")

    }
  }
}
