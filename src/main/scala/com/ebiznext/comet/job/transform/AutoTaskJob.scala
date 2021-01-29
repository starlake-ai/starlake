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

import com.ebiznext.comet.config.{Settings, StorageArea}
import com.ebiznext.comet.job.index.bqload.{BigQueryJobResult, BigQueryLoadConfig, BigQueryNativeJob}
import com.ebiznext.comet.job.ingest.{AuditLog, SparkAuditLogWriter, Step}
import com.ebiznext.comet.job.metrics.AssertionJob
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.Formatter._
import com.ebiznext.comet.utils.{JobResult, SparkJob, SparkJobResult, Utils}
import org.apache.hadoop.fs.Path

import java.io.{File, PrintStream}
import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}
import scala.util.{Failure, Success, Try}

/** Execute the SQL Task and store it in parquet/orc/.... If Hive support is enabled, also store it as a Hive Table.
  * If analyze support is active, also compute basic statistics for twhe dataset.
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
  views: Views,
  engine: Engine,
  task: AutoTaskDesc,
  storageHandler: StorageHandler,
  sqlParameters: Map[String, String],
  schemaHandler: SchemaHandler
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

  val (createDisposition, writeDisposition) =
    Utils.getDBDisposition(task.write, hasMergeKeyDefined = false)

  private def createConfig(): BigQueryLoadConfig = {
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
      val queryExpr = views.views
        .getOrElse(viewName, throw new Exception(s"View with name $viewName not found"))
      val bqNativeJob = new BigQueryNativeJob(
        config,
        "DUMMY - NOT EXECUTED",
        udf
      )

      val jsonQuery =
        s"SELECT TO_JSON_STRING(t,false) FROM (${queryExpr.richFormat(sqlParameters)}) AS t"
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
    val start = Timestamp.from(Instant.now())
    val subSelects: String = views.views.map { case (queryName, queryExpr) =>
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

    val jobResult: Try[JobResult] = bqNativeJob.run()
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
        jobResult map { jobResult =>
          val end = Timestamp.from(Instant.now())
          val jobResultCount =
            jobResult.asInstanceOf[BigQueryJobResult].tableResult.get.getTotalRows
          logAuditSuccess(start, end, jobResultCount)
          // We execute assertions only on success
          if (settings.comet.assertions.active) {
            task.area.orElse(defaultArea).foreach { area =>
              new AssertionJob(
                task.domain,
                area.value,
                task.assertions.getOrElse(Map.empty),
                Stage.UNIT,
                storageHandler,
                schemaHandler,
                None,
                engine,
                sql =>
                  bqNativeJob
                    .runSQL(sql.richFormat(sqlParameters))
                    .tableResult
                    .map(_.getTotalRows)
                    .getOrElse(0)
              ).run()
            }
          }
        }
        Success(BigQueryJobResult(None))
      case _ =>
        val err = errors.reduce(_.initCause(_))
        val end = Timestamp.from(Instant.now())
        logAuditFailure(start, end, err)
        Failure(err)
    }
  }

  def runSpark(): Try[SparkJobResult] = {
    val start = Timestamp.from(Instant.now())
    val res = Try {
      udf.foreach { udf =>
        registerUdf(udf)
      }
      createViews(views, sqlParameters, schemaHandler.activeEnv)

      task.presql
        .getOrElse(Nil)
        .foreach(req => session.sql(req.richFormat(sqlParameters)))
      val sqlWithParameters = task.sql.richFormat(sqlParameters)
      logger.info(s"running sql request $sqlWithParameters")
      val dataframe = session.sql(sqlWithParameters)

      val targetPath = task.getTargetPath(defaultArea)
      logger.info(s"About to write resulting dataset to $targetPath")
      // Target Path exist only if a storage area has been defined at task or job level
      // To execute a task without writing to disk simply avoid the area at the job and task level
      targetPath.foreach { targetPath =>
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
            val extension = format.getOrElse(settings.comet.defaultWriteFormat)
            val csvPath = storageHandler.list(targetPath, s".$extension", LocalDateTime.MIN).head
            val finalPath = new Path(targetPath, targetPath.getName + s".$extension")
            storageHandler.move(csvPath, finalPath)
          }
        }
        if (settings.comet.assertions.active) {
          task.area.orElse(defaultArea).foreach { area =>
            new AssertionJob(
              task.domain,
              area.value,
              task.assertions.getOrElse(Map.empty),
              Stage.UNIT,
              storageHandler,
              schemaHandler,
              Some(dataframe),
              engine,
              sql => session.sql(sql).count()
            ).run()
          }
        }
      }

      task.postsql.getOrElse(Nil).foreach(session.sql)
      // Let us return the Dataframe so that it can be piped to another sink
      SparkJobResult(Some(dataframe))
    }
    val end = Timestamp.from(Instant.now())
    res match {
      case Success(jobResult) =>
        val end = Timestamp.from(Instant.now())
        val jobResultCount = jobResult.asInstanceOf[SparkJobResult].dataframe.get.count()
        logAuditSuccess(start, end, jobResultCount)
      case Failure(e) =>
        logAuditFailure(start, end, e)
    }
    res
  }

  private def logAudit(
    start: Timestamp,
    end: Timestamp,
    jobResultCount: Long,
    success: Boolean,
    message: String
  ): Unit = {
    val log = AuditLog(
      session.sparkContext.applicationId,
      this.name,
      this.task.domain,
      this.task.dataset,
      success,
      -1,
      -1,
      jobResultCount,
      start,
      end.getTime - start.getTime,
      message,
      Step.TRANSFORM.toString
    )
    SparkAuditLogWriter.append(session, log)
  }

  private def logAuditSuccess(start: Timestamp, end: Timestamp, jobResultCount: Long) =
    logAudit(start, end, jobResultCount, true, "success")

  private def logAuditFailure(start: Timestamp, end: Timestamp, e: Throwable) =
    logAudit(start, end, -1, true, Utils.exceptionAsString(e))
}
