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
import com.ebiznext.comet.job.index.bqload.{BigQueryLoadConfig, BigQueryNativeJob}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.SinkType.{BQ, FS}
import com.ebiznext.comet.schema.model.{AutoTaskDesc, BigQuerySink, Engine, SinkType}
import com.ebiznext.comet.utils.Formatter._
import com.ebiznext.comet.utils.{SparkJob, Utils}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

/**
  * Execute the SQL Task and store it in parquet/orc/.... If Hive support is enabled, also store it as a Hive Table.
  * If analyze support is active, also compute basic statistics for the dataset.
  *
  * @param name        : Job Name as defined in the YML job description file
  * @param defaultArea : Where the resulting dataset is stored by default if not specified in the task
  * @param task        : Task to run
  * @param sqlParameters : Sql Parameters to pass to SQL statements
  */
class AutoTaskJob(
  override val name: String,
  defaultArea: Option[StorageArea],
  format: scala.Option[String],
  coalesce: Boolean,
  udf: scala.Option[String],
  views: scala.Option[Map[String, String]],
  engine: Engine,
  task: AutoTaskDesc,
  storageHandler: StorageHandler,
  sqlParameters: Map[String, String]
)(implicit val settings: Settings)
    extends SparkJob {

  override def run(): Try[Option[DataFrame]] = {
    engine match {
      case Engine.BQ =>
        runBQ()
      case Engine.SPARK =>
        runSpark()
      case _ =>
        throw new Exception("Should never happen !!!s")
    }
  }

  def runBQ(): Try[Option[DataFrame]] = {
    views.foreach(views => BigQueryNativeJob.createViews(views, udf))

    val (createDisposition, writeDisposition) =
      Utils.getDBDisposition(task.write, hasMergeKeyDefined = false)
    val bqSink = task.sink.map(sink => sink.asInstanceOf[BigQuerySink]).getOrElse(BigQuerySink())

    val config = BigQueryLoadConfig(
      outputTable = task.dataset,
      outputDataset = task.domain,
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      location = bqSink.location,
      outputPartition = bqSink.timestamp,
      outputClustering = bqSink.clustering.getOrElse(Nil),
      days = bqSink.days,
      requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
      rls = task.rls,
      engine = Engine.BQ
    )
    val bqNativeJob = new BigQueryNativeJob(config, task.sql.richFormat(sqlParameters), udf)

    Try {
      task.presql.getOrElse(Nil).foreach { sql =>
        bqNativeJob.runSQL(sql.richFormat(sqlParameters))
      }

      val jobResult = bqNativeJob.run()

      // We execute the post statements even if the main statement failed
      // They may be doing some cleanup
      task.postsql.getOrElse(Nil).foreach { sql =>
        bqNativeJob.runSQL(sql.richFormat(sqlParameters))
      }
      jobResult match {
        case Failure(exception) => throw exception
        case Success(_)         => None
      }
    }
  }

  def runSpark(): Try[Option[DataFrame]] = {
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
            (SinkType.fromString(value.substring(0, sepIndex)), value.substring(sepIndex + 1))
          else // parquet is the default
            (SinkType.FS, value)

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
            throw new Exception("Should never happen")
        }
        df.createOrReplaceTempView(key)
    }

    task.presql.getOrElse(Nil).foreach(req => session.sql(req.richFormat(sqlParameters)))
    val dataframe = session.sql(task.sql.richFormat(sqlParameters))

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
          analyze(fullTableName)
        }
      } else {
        finalDataset.save()
        if (coalesce) {
          val csvPath = storageHandler.list(targetPath, ".csv", LocalDateTime.MIN).head
          val finalCsvPath = new Path(targetPath, targetPath.getName + ".csv")
          storageHandler.move(csvPath, finalCsvPath)
        }
      }
    }

    task.postsql.getOrElse(Nil).foreach(session.sql)
    // Let us return the Dataframe so that it can be piped to another sink
    Success(Some(dataframe))
  }
}
