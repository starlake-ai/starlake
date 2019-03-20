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

import com.ebiznext.comet.config.{DatasetArea, HiveArea, Settings}
import com.ebiznext.comet.job.index.IndexConfig
import com.ebiznext.comet.schema.model.AutoTask
import com.ebiznext.comet.utils.SparkJob
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
  *
  * Execute the SQL Task and store it in parquet/orc/.... If Hive support is enabled, also store it as a Hive Table.
  * If analyze support is active, also compute basic statistics for the dataset.
  *
  * @param name        : Job Name as defined in the YML job description file
  * @param defaultArea : Where the resulting dataset is stored by default if not specified in the task
  * @param task        : Task to run
  */
class AutoJob(override val name: String, defaultArea: HiveArea, task: AutoTask) extends SparkJob {

  def run(): SparkSession = {
    if (Settings.comet.hive) {
      task.presql.getOrElse(Nil).foreach(session.sql)
      val targetArea = task.area.getOrElse(defaultArea)
      val hiveDB = HiveArea.area(task.domain, targetArea)
      val tableName = task.dataset
      val fullTableName = s"$hiveDB.$tableName"
      session.sql(s"create database if not exists $hiveDB")
      session.sql(s"use $hiveDB")
      session.sql(s"drop table if exists $tableName")
      val dataframe = session.sql(task.sql)
      val targetPath =
        new Path(DatasetArea.path(task.domain, targetArea.value), task.dataset)
      val partitionedDF =
        partitionedDatasetWriter(dataframe, task.getPartitions())
      partitionedDF
        .mode(task.write.toSaveMode)
        .format(Settings.comet.writeFormat)
        .option("path", targetPath.toString)
        .saveAsTable(fullTableName)
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
      task.postsql.getOrElse(Nil).foreach(session.sql)
    }
    session
  }
}
