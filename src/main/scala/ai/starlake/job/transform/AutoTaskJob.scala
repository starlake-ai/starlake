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

package ai.starlake.job.transform

import ai.starlake.config.{Settings, StorageArea}
import ai.starlake.job.sink.bigquery.{BigQueryJobResult, BigQueryLoadConfig, BigQueryNativeJob}
import ai.starlake.job.ingest.{AuditLog, Step}
import ai.starlake.job.metrics.AssertionJob
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.Stage.UNIT
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}
import scala.util.{Failure, Success, Try}

/** Execute the SQL Task and store it in parquet/orc/.... If Hive support is enabled, also store it
  * as a Hive Table. If analyze support is active, also compute basic statistics for twhe dataset.
  *
  * @param name
  *   : Job Name as defined in the YML job description file
  * @param defaultArea
  *   : Where the resulting dataset is stored by default if not specified in the task
  * @param task
  *   : Task to run
  * @param sqlParameters
  *   : Sql Parameters to pass to SQL statements
  */
case class AutoTaskJob(
  override val name: String,
  defaultArea: StorageArea,
  format: scala.Option[String],
  coalesce: Boolean,
  udf: scala.Option[String],
  views: Views,
  engine: Engine,
  task: AutoTaskDesc,
  sqlParameters: Map[String, String],
  sink: Option[Sink],
  interactive: Option[String]
)(implicit val settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends SparkJob {

  override def run(): Try[JobResult] = {
    throw new Exception("Should never happen !!! Call runBQ or runSpark directly")
  }

  val (createDisposition, writeDisposition) =
    Utils.getDBDisposition(task.write, hasMergeKeyDefined = false)

  private def createBigQueryConfig(): BigQueryLoadConfig = {
    val bqSink = task.sink.map(sink => sink.asInstanceOf[BigQuerySink]).getOrElse(BigQuerySink())
    BigQueryLoadConfig(
      outputTable = task.table,
      outputDataset = task.domain,
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      location = bqSink.location,
      outputPartition = bqSink.timestamp,
      outputClustering = bqSink.clustering.getOrElse(Nil),
      days = bqSink.days,
      requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
      rls = task.rls,
      engine = Engine.BQ,
      options = bqSink.getOptions
    )
  }

  private def parseJobViews(): Map[String, String] =
    views.views.map { case (queryName, queryExpr) =>
      val (_, _, viewValue) =
        parseViewDefinition(queryExpr.richFormat(schemaHandler.activeEnv, sqlParameters))
      (queryName, viewValue)
    }

  def parseMainSqlBQ(): JdbcConfigName = {
    logger.info(s"Parse Views")
    val withViews = parseJobViews()
    val mainTaskSQL =
      CommentParser.stripComments(
        task.getSql().richFormat(schemaHandler.activeEnv, sqlParameters).trim
      )
    val trimmedMainTaskSQL = mainTaskSQL.toLowerCase().trim
    if (trimmedMainTaskSQL.startsWith("with ") || trimmedMainTaskSQL.startsWith("(with ")) {
      mainTaskSQL
    } else {
      val subSelects = withViews.map { case (queryName, queryExpr) =>
        val selectExpr =
          if (queryExpr.toLowerCase.startsWith("select "))
            queryExpr
          else {
            val allColumns = "*"
            s"SELECT $allColumns FROM $queryExpr"
          }
        queryName + " AS (" + selectExpr + ")"
      }
      val subSelectsString = if (subSelects.nonEmpty) subSelects.mkString("WITH ", ",", " ") else ""
      "(\n" + subSelectsString + mainTaskSQL + "\n)"
    }
  }

  def buildQueryBQ(): (List[String], String, List[String]) = {
    val sql = parseMainSqlBQ()
    val preSql = task.presql.getOrElse(Nil).map { sql =>
      sql.richFormat(schemaHandler.activeEnv, sqlParameters)
    }
    val postSql = task.postsql.getOrElse(Nil).map { sql =>
      sql.richFormat(schemaHandler.activeEnv, sqlParameters)
    }
    (preSql, sql, postSql)
  }

  def runBQ(): Try[JobResult] = {
    val start = Timestamp.from(Instant.now())
    logger.info(s"running BQ Query  start time $start")
    val config = createBigQueryConfig()
    logger.info(s"running BQ Query with config $config")
    val (preSql, mainSql, postSql) = buildQueryBQ()
    logger.info(s"Config $config")
    // We add extra parenthesis required by BQ when using "WITH" keyword
    def bqNativeJob(sql: String) =
      new BigQueryNativeJob(config, if (sql.trim.startsWith("(")) sql else "(" + sql + ")", udf)

    val presqlResult: List[Try[JobResult]] =
      preSql.map { sql =>
        logger.info(s"Running PreSQL BQ Query: $sql")
        bqNativeJob(sql).runInteractiveQuery()
      }
    presqlResult.foreach(Utils.logFailure(_, logger))

    logger.info(s"""START COMPILE SQL $mainSql END COMPILE SQL""")
    val jobResult: Try[JobResult] = interactive match {
      case None =>
        bqNativeJob(mainSql).run()
      case Some(_) =>
        bqNativeJob(mainSql).runInteractiveQuery()
    }

    Utils.logFailure(jobResult, logger)

    // We execute the post statements even if the main statement failed
    // We may be doing some cleanup here.

    val postsqlResult: List[Try[JobResult]] =
      postSql.map { sql =>
        logger.info(s"Running PostSQL BQ Query: $sql")
        bqNativeJob(sql).runInteractiveQuery()
      }
    postsqlResult.foreach(Utils.logFailure(_, logger))

    val errors =
      (presqlResult ++ List(jobResult) ++ postsqlResult).map(_.failed).collect { case Success(e) =>
        e
      }
    errors match {
      case Nil =>
        jobResult map { jobResult =>
          val end = Timestamp.from(Instant.now())
          val jobResultCount =
            jobResult.asInstanceOf[BigQueryJobResult].tableResult.map(_.getTotalRows)
          jobResultCount.foreach(logAuditSuccess(start, end, _))
          // We execute assertions only on success
          if (settings.comet.assertions.active) {
            new AssertionJob(
              task.domain,
              task.area.getOrElse(defaultArea).value,
              task.assertions.getOrElse(Map.empty),
              UNIT,
              storageHandler,
              schemaHandler,
              None,
              engine,
              sql =>
                bqNativeJob(sql.richFormat(schemaHandler.activeEnv, sqlParameters))
                  .runInteractiveQuery()
                  .map { result =>
                    val bqResult = result.asInstanceOf[BigQueryJobResult]
                    bqResult.tableResult
                      .map(_.getTotalRows)
                      .getOrElse(0L)
                  }
                  .getOrElse(0L)
            ).run()
          }
        }
        jobResult
      case _ =>
        val err = errors.reduce(_.initCause(_))
        val end = Timestamp.from(Instant.now())
        logAuditFailure(start, end, err)
        Failure(err)
    }
  }

  def buildQuerySpark(): (List[String], String, List[String]) = {
    val preSql = task.presql.getOrElse(Nil).map { sql =>
      sql.richFormat(schemaHandler.activeEnv, sqlParameters)
    }
    val sql = task.getSql().richFormat(schemaHandler.activeEnv, sqlParameters)
    val postSql = task.postsql.getOrElse(Nil).map { sql =>
      sql.richFormat(schemaHandler.activeEnv, sqlParameters)
    }
    (preSql, sql, postSql)
  }

  def sinkToFS(dataframe: DataFrame, sink: FsSink): Boolean = {
    val targetPath = task.getTargetPath(defaultArea)
    logger.info(s"About to write resulting dataset to $targetPath")
    // Target Path exist only if a storage area has been defined at task or job level
    // To execute a task without writing to disk simply avoid the area at the job and task level

    val sinkPartition =
      sink.partition.getOrElse(Partition(sampling = None, attributes = task.partition))

    val sinkPartitionSampling = sinkPartition.sampling.getOrElse(0.0)
    val nbPartitions = sinkPartitionSampling match {
      case 0.0 =>
        dataframe.rdd.getNumPartitions
      case count if count >= 1.0 =>
        count.toInt
      case count =>
        throw new Exception(s"Invalid partition value $count in Sink $sink")
    }

    val partitionedDF =
      if (coalesce)
        dataframe.repartition(1)
      else if (sinkPartitionSampling == 0)
        dataframe
      else
        dataframe.repartition(nbPartitions)

    val partitionedDFWriter =
      partitionedDatasetWriter(
        partitionedDF,
        sinkPartition.attributes.getOrElse(Nil)
      )

    val clusteredDFWriter = sink.clustering match {
      case None          => partitionedDFWriter
      case Some(columns) => partitionedDFWriter.sortBy(columns.head, columns.tail: _*)
    }

    val finalDataset = clusteredDFWriter
      .mode(task.write.toSaveMode)
      .format(sink.format.getOrElse(format.getOrElse(settings.comet.defaultFormat)))
      .options(sink.getOptions)
      .option("path", targetPath.toString)
      .options(sink.getOptions)

    if (settings.comet.hive) {
      val tableName = task.table
      val hiveDB = task.getHiveDB(defaultArea)
      val fullTableName = s"$hiveDB.$tableName"
      session.sql(s"create database if not exists $hiveDB")
      session.sql(s"use $hiveDB")
      if (task.write.toSaveMode == SaveMode.Overwrite)
        session.sql(s"drop table if exists $tableName")
      finalDataset.saveAsTable(fullTableName)
      analyze(fullTableName)
    } else {
      // TODO Handle SinkType.FS and SinkType to Hive in Sink section in the caller

      finalDataset.save()
      if (coalesce) {
        val extension = sink.format.getOrElse(format.getOrElse(settings.comet.defaultFormat))
        val csvPath = storageHandler
          .list(targetPath, s".$extension", LocalDateTime.MIN, recursive = false)
          .head
        val finalPath = new Path(targetPath, targetPath.getName + s".$extension")
        storageHandler.move(csvPath, finalPath)
      }
    }
    true
  }

  def runSpark(): Try[SparkJobResult] = {
    val start = Timestamp.from(Instant.now())
    val res = Try {
      udf.foreach { udf =>
        registerUdf(udf)
      }
      createSparkViews(views, schemaHandler.activeEnv, sqlParameters)

      val (preSql, sqlWithParameters, postSql) = buildQuerySpark()

      preSql.foreach(req => session.sql(req))
      logger.info(s"""START COMPILE SQL $sqlWithParameters END COMPILE SQL""")
      logger.info(s"running sql request using ${task.engine}")

      val dataframe =
        task.engine.getOrElse(Engine.SPARK) match {
          case Engine.BQ =>
            session.read
              .format("com.google.cloud.spark.bigquery")
              .option("query", sqlWithParameters)
              .load()
          case Engine.SPARK => session.sql(sqlWithParameters)
          case Engine.JDBC =>
            logger.warn("JDBC Engine not supported on job task. Running query using Spark Engine")
            session.sql(sqlWithParameters)
          case _ => throw new Exception("should never happen")
        }

      if (settings.comet.hive || settings.comet.sinkToFile)
        sinkToFS(dataframe, FsSink())

      if (settings.comet.assertions.active) {
        new AssertionJob(
          task.domain,
          task.area.getOrElse(defaultArea).value,
          task.assertions.getOrElse(Map.empty),
          Stage.UNIT,
          storageHandler,
          schemaHandler,
          Some(dataframe),
          engine,
          sql => session.sql(sql).count()
        ).run()
      }

      postSql.foreach(req => session.sql(req))
      // Let us return the Dataframe so that it can be piped to another sink
      SparkJobResult(Some(dataframe))
    }
    val end = Timestamp.from(Instant.now())
    res match {
      case Success(jobResult) =>
        val end = Timestamp.from(Instant.now())
        val jobResultCount = jobResult.dataframe.map(_.count())
        jobResultCount.foreach(logAuditSuccess(start, end, _))
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
      this.task.table,
      success,
      jobResultCount,
      -1,
      -1,
      start,
      end.getTime - start.getTime,
      message,
      Step.TRANSFORM.toString
    )
    AuditLog.sink(session, log)
  }

  private def logAuditSuccess(start: Timestamp, end: Timestamp, jobResultCount: Long) =
    logAudit(start, end, jobResultCount, success = true, "success")

  private def logAuditFailure(start: Timestamp, end: Timestamp, e: Throwable) =
    logAudit(start, end, -1, success = true, Utils.exceptionAsString(e))
}
