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

import java.io.{File, PrintStream}
import java.time.LocalDateTime

import com.ebiznext.comet.config.{Settings, StorageArea, UdfRegistration}
import com.ebiznext.comet.job.index.bqload.{
  BigQueryJobResult,
  BigQueryLoadConfig,
  BigQueryNativeJob
}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.SinkType.{BQ, FS}
import com.ebiznext.comet.schema.model.{AutoTaskDesc, BigQuerySink, Engine, SinkType}
import com.ebiznext.comet.utils.Formatter._
import com.ebiznext.comet.utils.{JobResult, SparkJob, SparkJobResult, Utils}
import org.apache.hadoop.fs.Path

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

  override def run(): Try[JobResult] = {
    engine match {
      case Engine.BQ =>
        runBQ()
      case Engine.SPARK =>
        runSpark()
      case _ =>
        throw new Exception("Should never happen !!!s")
    }
  }

  private def createConfig(): BigQueryLoadConfig = {
    val (createDisposition, writeDisposition) =
      Utils.getDBDisposition(task.write, hasMergeKeyDefined = false)
    val bqSink = task.sink.map(sink => sink.asInstanceOf[BigQuerySink]).getOrElse(BigQuerySink())

    BigQueryLoadConfig(
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
  }

  def runView(viewName: String, viewDir: Option[String], viewCount: Int): Try[JobResult] = {
    Try {
      val config = createConfig()
      val queryExpr = views
        .getOrElse(Map.empty)
        .getOrElse(viewName, throw new Exception(s"View with name $viewName not found"))
      val bqNativeJob = new BigQueryNativeJob(
        config,
        "DUMMY - NOT EXECUTED",
        udf
      )

      val jsonQuery = s"SELECT TO_JSON_STRING(t,false) FROM (${queryExpr.richFormat(sqlParameters)}) AS t"
      val result = bqNativeJob.runSQL(jsonQuery.richFormat(sqlParameters))
      import scala.collection.JavaConverters._
      result.tableResult.foreach { tableResult =>
        var count = 0
        val it = tableResult.iterateAll().iterator().asScala
        val file = viewDir
          .map { dir =>
            new File(dir).mkdirs()
            new PrintStream(new File(dir, s"$viewName.json"), "UTF-8")
          }
          .getOrElse(System.out)
        while (it.hasNext && count < viewCount) {
          val item = it.next().get(0).getStringValue
          file.println(item)
          count = count + 1
        }
        file.close()
      }
      result
    }
  }

  def runBQ(): Try[JobResult] = {
    val subSelects: String = views.getOrElse(Map.empty).map { case (queryName, queryExpr) =>
      queryName + " AS (" + queryExpr.richFormat(sqlParameters) + ")"
    } mkString ("WITH ", ",", " ")

    val config = createConfig()

    val bqNativeJob = new BigQueryNativeJob(
      config,
      task.sql.richFormat(sqlParameters + ("views" -> subSelects)),
      udf
    )

    val presqlResult: Try[Iterable[BigQueryJobResult]] = Try {
      task.presql.getOrElse(Nil).map { sql =>
        bqNativeJob.runSQL(sql.richFormat(sqlParameters))
      }
    }
    Utils.logFailure(presqlResult, logger)

    val jobResult: Try[JobResult] =
      bqNativeJob.run()
    Utils.logFailure(jobResult, logger)

    // We execute the post statements even if the main statement failed
    // We may be doing some cleanup here.
    val postsqlResult: Try[Iterable[BigQueryJobResult]] = Try {
      task.postsql.getOrElse(Nil).map { sql =>
        bqNativeJob.runSQL(sql.richFormat(sqlParameters))
      }
    }
    Utils.logFailure(postsqlResult, logger)

    val errors =
      Iterable(presqlResult, jobResult, postsqlResult).filter(_.isFailure).map(_.failed).map(_.get)
    errors match {
      case Nil =>
        Success(BigQueryJobResult(None))
      case _ =>
        Failure(errors.reduce(_.initCause(_)))
    }
  }

  def runSpark(): Try[SparkJobResult] = {
    udf.foreach { udf =>
      val udfInstance: UdfRegistration =
        Class
          .forName(udf)
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[UdfRegistration]
      udfInstance.register(session)
    }
    views.getOrElse(Map()).foreach { case (key, value) =>
      val sepIndex = value.indexOf(":")
      val (format, path) =
        if (sepIndex > 0)
          (SinkType.fromString(value.substring(0, sepIndex)), value.substring(sepIndex + 1))
        else // parquet is the default
          (SinkType.FS, value)
      logger.info(s"Loading view $path from $format")
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
      logger.info(s"Created view $key")
    }

    task.presql
      .getOrElse(Nil)
      .foreach(req => session.sql(req.richFormat(sqlParameters)))
    val sqlWithParameters = task.sql.richFormat(sqlParameters)
    logger.info(s"running sql request $sqlWithParameters")
    val dataframe = session.sql(sqlWithParameters)

    val targetPath = task.getTargetPath(defaultArea)
    logger.info(s"About to write resulting dataset to $targetPath")
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
    Success(SparkJobResult(Some(dataframe)))
  }
}
