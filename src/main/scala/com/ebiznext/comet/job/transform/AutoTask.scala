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
import com.ebiznext.comet.utils.SparkJob
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.{Success, Try}

/**
  *
  * Execute the SQL Task and store it in parquet/orc/.... If Hive support is enabled, also store it as a Hive Table.
  * If analyze support is active, also compute basic statistics for the dataset.
  *
  * @param name        : Job Name as defined in the YML job description file
  * @param defaultArea : Where the resulting dataset is stored by default if not specified in the task
  * @param task        : Task to run
  */
class AutoTask(
  override val name: String,
  defaultArea: StorageArea,
  format: Option[String],
  coalesce: Boolean,
  udf: Option[String],
  views: Option[Map[String, String]],
  task: AutoTaskDesc,
  storageHandler: StorageHandler
) extends SparkJob {

  def run(): Try[SparkSession] = {
    udf.foreach { udf =>
      val udfInstance: UdfRegistration =
        Class.forName(udf).newInstance.asInstanceOf[UdfRegistration]
      udfInstance.register(session)
    }
    views.getOrElse(Map()).foreach {
      case (key, value) =>
        val fullPath = if (value.startsWith("/")) value else s"${Settings.comet.datasets}/$value"
        val df = session.read.parquet(fullPath)
        df.createOrReplaceTempView(key)
    }
    task.presql.getOrElse(Nil).foreach(session.sql)
    val targetPath = task.getTargetPath(defaultArea)
    val mergePath = s"${targetPath.toString}.merge"

    val dataframe = session.sql(task.sql)
    val partitionedDF =
      partitionedDatasetWriter(
        if (coalesce) dataframe.coalesce(1) else dataframe,
        task.getPartitions()
      )
    partitionedDF
      .mode(SaveMode.Overwrite)
      .format(Settings.comet.writeFormat)
      .option("path", mergePath)
      .save()

    val finalDataset = partitionedDF
      .mode(task.write.toSaveMode)
      .format(format.getOrElse(Settings.comet.writeFormat))
      .option("path", targetPath.toString)

    val _ = storageHandler.delete(new Path(mergePath))
    if (Settings.comet.hive) {
      val tableName = task.dataset
      val hiveDB = task.getHiveDB(defaultArea)
      val fullTableName = s"$hiveDB.$tableName"
      session.sql(s"create database if not exists $hiveDB")
      session.sql(s"use $hiveDB")
      session.sql(s"drop table if exists $tableName")
      finalDataset.saveAsTable(fullTableName)
      if (Settings.comet.analyze) {
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
    task.postsql.getOrElse(Nil).foreach(session.sql)
    Success(session)
  }
}

/*
name: "facturation"
udf: "com.ebiznext.comet.external.EvolanUdf"
format: "csv"
coalesce: true
views:
  ref_client: "/trainings/jupyter/work/ebiznext/business/patrice/client"
  ref_societe: "/trainings/jupyter/work/ebiznext/accepted/patrice/societe"
tasks:
  - sql: |
      select
        clients.INTITULE_TIERS,
        clients.CODE_POSTAL,

      from  ref_client clients
      where
        clients.CODE_SOCIETE in (1, 2? 3, 4)
    domain: "factu"
    dataset: "facturation"
    write: "OVERWRITE"
    area: "business"
 */
