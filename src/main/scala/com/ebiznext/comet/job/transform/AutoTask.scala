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

import com.ebiznext.comet.config.{Settings, StorageArea, UdfRegistration}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.IndexSink._
import com.ebiznext.comet.schema.model.{AutoTaskDesc, IndexSink}
import com.ebiznext.comet.utils.Formatter._
import com.ebiznext.comet.utils.{SparkJob, SparkJobResult}
import org.apache.hadoop.fs.Path

import scala.language.reflectiveCalls
import scala.util.{Success, Try}

/**
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
  defaultArea: Option[StorageArea],
  format: Option[String],
  coalesce: Boolean,
  udf: Option[String],
  views: Option[Map[String, String]],
  task: AutoTaskDesc,
  storageHandler: StorageHandler,
  sqlParameters: Option[Map[String, String]]
)(implicit val settings: Settings)
    extends SparkJob {

  def run(): Try[SparkJobResult] = {
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
            (IndexSink.fromString(value.substring(0, sepIndex)), value.substring(sepIndex + 1))
          else // parquet is the default
            (IndexSink.FS, value)

        val df = format match {
          case FS =>
            val fullPath =
              if (path.startsWith("/")) path else s"${settings.comet.datasets}/$path"
            session.read.parquet(fullPath)
          case BQ =>
            session.read
              .format("com.google.cloud.spark.bigquery")
              .load(path)
              .cache()
          case _ =>
            ???
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
    val targetPath = task.getTargetPath(defaultArea)
    // Target Path exist only if a storage area has been defined at task or job level
    targetPath.map { targetPath =>
      val partitionedDF =
        partitionedDatasetWriter(
          if (coalesce) dataframe.coalesce(1) else dataframe,
          task.getPartitions()
        )

      val finalDataset = partitionedDF
        .mode(task.write.toSaveMode)
        .format(format.getOrElse(settings.comet.defaultWriteFormat))
        .option("path", targetPath.toString)

      if (settings.comet.hive) {
        val tableName = task.dataset
        val hiveDB = task.getHiveDB(defaultArea)
        hiveDB.map { hiveDB =>
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
    // Let us return the Dataframe so that it can be piped to another sink
    Success(SparkJobResult(session, Some(dataframe)))
  }
}
