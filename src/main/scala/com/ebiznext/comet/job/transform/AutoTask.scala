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

package com.ebiznext.comet.job.transform

import java.time.LocalDateTime

import com.ebiznext.comet.config.{DatasetArea, Settings, StorageArea, UdfRegistration}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.AutoTaskDesc
import com.ebiznext.comet.utils.Formatter._
import com.ebiznext.comet.utils.SparkJob
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.language.reflectiveCalls
import scala.util.{Success, Try}

/**
  *
  * Execute the SQL Task and store it in parquet/orc/.... If Hive support is enabled, also store it as a Hive Table.
  * If analyze support is active, also compute basic statistics for the dataset.
  *
  * @param name        : Job Name as defined in the YML job description file
  * @param defaultArea : Where the resulting dataset is stored by default if not specified in the task
  * @param task        : Task to run
  * @param sqlParameters : Sql Parameters to pass to SQL statements
  */
class AutoTask(
  override val name: String,
  defaultArea: StorageArea,
  format: Option[String],
  coalesce: Boolean,
  udf: Option[String],
  views: Option[Map[String, String]],
  task: AutoTaskDesc,
  storageHandler: StorageHandler,
  sqlParameters: Option[Map[String, String]]
)(implicit val settings: Settings)
    extends SparkJob {

  def run(): Try[SparkSession] = {
    udf.foreach { udf =>
      val udfInstance: UdfRegistration =
        Class
          .forName(udf)
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[UdfRegistration]
      udfInstance.register(session)
    }
    views.getOrElse(Map()).foreach {
      case (key, value) =>
        val sepIndex = value.indexOf(":")
        val (format, path) =
          if (sepIndex > 0)
            (value.substring(0, sepIndex), value.substring(sepIndex + 1))
          else // parquet is the default
            ("parquet", value)
        val df = format match {
          case "parquet" =>
            val fullPath =
              if (path.startsWith("/")) path else s"${settings.comet.datasets}/$path"
            session.read.parquet(fullPath)
          case "bigquery" =>
            session.read
              .format("com.google.cloud.spark.bigquery")
              .option("table", path)
              .load()
              .cache()
        }
        df.createOrReplaceTempView(key)
    }

    val dataframe = sqlParameters match {
      case Some(mapParams) =>
        task.presql.getOrElse(Nil).foreach(req => session.sql(req.richFormat(mapParams)))
        session.sql(task.sql.richFormat(mapParams))
      case _ =>
        task.presql.getOrElse(Nil).foreach(session.sql)
        session.sql(task.sql)

    }

    val taskArea = task.area.getOrElse(defaultArea)
    taskArea.storageType match {
      case "bigquery" =>
        (taskArea.storageType, taskArea.storageValue)
        dataframe.write
          .format("com.google.cloud.spark.bigquery")
          .option("table", taskArea.storageValue)
          .save()
      case "parquet" =>
        val targetPath = task.getTargetPath(defaultArea)
        val mergePath = s"${targetPath.toString}.merge"
        val partitionedDF =
          partitionedDatasetWriter(
            if (coalesce) dataframe.coalesce(1) else dataframe,
            task.getPartitions()
          )
        partitionedDF
          .mode(SaveMode.Overwrite)
          .format(settings.comet.writeFormat)
          .option("path", mergePath)
          .save()

        val finalDataset = partitionedDF
          .mode(task.write.toSaveMode)
          .format(format.getOrElse(settings.comet.writeFormat))
          .option("path", targetPath.toString)

        val _ = storageHandler.delete(new Path(mergePath))
        if (settings.comet.hive) {
          val tableName = task.dataset
          val hiveDB = task.getHiveDB(defaultArea)
          val fullTableName = s"$hiveDB.$tableName"
          session.sql(s"create database if not exists $hiveDB")
          session.sql(s"use $hiveDB")
          session.sql(s"drop table if exists $tableName")
          finalDataset.saveAsTable(fullTableName)
          if (settings.comet.analyze) {
            val allCols = session.table(fullTableName).columns.mkString(",")
            val analyzeTable =
              s"ANALYZE TABLE $fullTableName COMPUTE STATISTICS FOR COLUMNS $allCols"
            if (session.version.substring(0, 3).toDouble >= 2.4)
              try {
                session.sql(analyzeTable)
              } catch {
                case e: Throwable =>
                  logger.warn(
                    s"Failed to compute statistics for table $fullTableName on columns $allCols"
                  )
                  e.printStackTrace()
              }
          }
        } else {
          finalDataset.save()
          if (coalesce) {
            val csvPath = storageHandler.list(targetPath, ".csv", LocalDateTime.MIN).head
            val finalCsvPath = new Path(targetPath, targetPath.getName() + ".csv")
            storageHandler.move(csvPath, finalCsvPath)
          }
        }
    }
    task.postsql.getOrElse(Nil).foreach(session.sql)
    Success(session)
  }
}
