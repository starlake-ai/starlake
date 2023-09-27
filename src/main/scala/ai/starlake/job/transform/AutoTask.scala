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

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.ingest.{AuditLog, Step}
import ai.starlake.job.metrics.{
  BigQueryExpectationAssertionHandler,
  ExpectationJob,
  SparkExpectationAssertionHandler
}
import ai.starlake.job.sink.bigquery.{
  BigQueryJobBase,
  BigQueryJobResult,
  BigQueryLoadConfig,
  BigQueryNativeJob
}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils._
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.PythonRunner
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}
import scala.util.{Failure, Success, Try}

object AutoTask extends StrictLogging {
  def unauthenticatedTasks(reload: Boolean)(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): List[AutoTask] = {
    schemaHandler
      .tasks(reload)
      .map(task(_, Map.empty, None))
  }

  def task(
    taskDesc: AutoTaskDesc,
    configOptions: Map[String, String],
    interactive: Option[String],
    resultPageSize: Int = 1
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): AutoTask = {
    AutoTask(
      taskDesc,
      configOptions,
      taskDesc.sink
        .map(_.getSink())
        .orElse(Some(AllSinks().getSink())),
      interactive,
      taskDesc.getDatabase(),
      resultPageSize = resultPageSize
    )
  }
}

/** Execute the SQL Task and store it in parquet/orc/.... If Hive support is enabled, also store it
  * as a Hive Table. If analyze support is active, also compute basic statistics for twhe dataset.
  *
  * @param name
  *   : Job Name as defined in the YML job description file
  * @param defaultArea
  *   : Where the resulting dataset is stored by default if not specified in the task
  * @param taskDesc
  *   : Task to run
  * @param commandParameters
  *   : Sql Parameters to pass to SQL statements
  */
case class AutoTask(
  taskDesc: AutoTaskDesc,
  commandParameters: Map[String, String],
  sink: Option[Sink],
  interactive: Option[String],
  database: Option[String],
  resultPageSize: Int = 1
)(implicit val settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends SparkJob {
  override def name: String = taskDesc.name
  override def run(): Try[JobResult] = {
    throw new Exception("Should never happen !!! Call runBQ or runSpark directly")
  }

  val (createDisposition, writeDisposition) =
    Utils.getDBDisposition(taskDesc.getWrite(), hasMergeKeyDefined = false)

  private def createBigQueryConfig(): BigQueryLoadConfig = {
    val connectionRef = sink.flatMap(_.connectionRef).getOrElse(settings.appConfig.connectionRef)
    val bqSink =
      taskDesc.sink
        .map(_.getSink())
        .getOrElse(BigQuerySink(connectionRef = Some(connectionRef)))
        .asInstanceOf[BigQuerySink]
    BigQueryLoadConfig(
      connectionRef = Some(connectionRef),
      outputTableId = Some(
        BigQueryJobBase
          .extractProjectDatasetAndTable(taskDesc.database, taskDesc.domain, taskDesc.table)
      ),
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      outputPartition = bqSink.timestamp,
      outputClustering = bqSink.clustering.getOrElse(Nil),
      days = bqSink.days,
      requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
      rls = taskDesc.rls,
      engine = Engine.BQ,
      acl = taskDesc.acl,
      materializedView = taskDesc.sink
        .map(_.getSink())
        .exists(sink => sink.asInstanceOf[BigQuerySink].materializedView.getOrElse(false)),
      enableRefresh = bqSink.enableRefresh,
      refreshIntervalMs = bqSink.refreshIntervalMs,
      attributesDesc = taskDesc.attributesDesc,
      outputTableDesc = taskDesc.comment,
      starlakeSchema = Some(Schema.fromTaskDesc(taskDesc)),
      outputDatabase = taskDesc.getDatabase()
    )
  }

  def runBQ(drop: Boolean): Try[JobResult] = {
    val config = createBigQueryConfig()
    def bqNativeJob(sql: String): BigQueryNativeJob = {
      val toUpperSql = sql.toUpperCase()
      val finalSql =
        if (toUpperSql.startsWith("WITH") || toUpperSql.startsWith("SELECT"))
          "(" + sql + ")"
        else
          sql
      new BigQueryNativeJob(config, finalSql, this.resultPageSize)
    }

    val start = Timestamp.from(Instant.now())
    if (drop) {
      logger.info(s"Truncating table ${taskDesc.domain}.${taskDesc.table}")
      bqNativeJob("ignore sql").dropTable(
        taskDesc.getDatabase(),
        taskDesc.domain,
        taskDesc.table
      )
    }
    logger.info(s"running BQ Query  start time $start")
    val tableExists =
      bqNativeJob("ignore sql").tableExists(
        taskDesc.getDatabase(),
        taskDesc.domain,
        taskDesc.table
      )
    logger.info(s"running BQ Query with config $config")
    val (preSql, mainSql, postSql) = buildAllSQLQueries(tableExists, Nil)
    logger.info(s"Config $config")
    // We add extra parenthesis required by BQ when using "WITH" keyword

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
          if (settings.appConfig.expectations.active) {
            new ExpectationJob(
              taskDesc.database,
              taskDesc.domain,
              taskDesc.table,
              taskDesc.expectations,
              storageHandler,
              schemaHandler,
              None,
              taskDesc.getEngine(),
              new BigQueryExpectationAssertionHandler(bqNativeJob(""))
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

  def buildAllSQLQueries(
    tableExists: Boolean,
    localViews: List[String]
  ): (List[String], String, List[String]) = {
    // available jinja variables to build sql query depending on
    // whether the table exists or not.
    val allVars = schemaHandler.activeEnvVars() ++ commandParameters ++ Map("merge" -> tableExists)
    val sql = Utils.parseJinja(taskDesc.getSql(), allVars)
    val mergeSql =
      if (!tableExists) {
        if (taskDesc.parseSQL.getOrElse(true))
          SQLUtils.buildSingleSQLQuery(
            sql,
            schemaHandler.refs(),
            schemaHandler.domains(),
            schemaHandler.tasks(),
            localViews,
            taskDesc.getEngine()
          )
        else
          sql
      } else {
        taskDesc.merge match {
          case Some(options) =>
            val mergeSql =
              SQLUtils.buildMergeSql(
                sql,
                options.key,
                taskDesc.getDatabase(),
                taskDesc.domain,
                taskDesc.table,
                taskDesc.getEngine(),
                localViews.nonEmpty
              )
            logger.info(s"Merge SQL: $mergeSql")
            mergeSql
          case None =>
            if (taskDesc.parseSQL.getOrElse(true))
              SQLUtils.buildSingleSQLQuery(
                sql,
                schemaHandler.refs(),
                schemaHandler.domains(),
                schemaHandler.tasks(),
                localViews,
                taskDesc.getEngine()
              )
            else
              sql
        }
      }

    val preSql = parseJinja(taskDesc.presql, allVars)
    val postSql = parseJinja(taskDesc.postsql, allVars)

    (preSql, s"(\n$mergeSql\n)", postSql)
  }

  private def parseJinja(sql: String, vars: Map[String, Any]): String = parseJinja(
    List(sql),
    vars
  ).head

  /** All variables defined in the active profile are passed as string parameters to the Jinja
    * parser.
    * @param sqls
    * @return
    */
  private def parseJinja(sqls: List[String], vars: Map[String, Any]): List[String] = {
    val result = Utils
      .parseJinja(sqls, schemaHandler.activeEnvVars() ++ commandParameters ++ vars)
    logger.info(s"Parse Jinja result: $result")
    result
  }

  def sinkToFS(dataframe: DataFrame, sink: FsSink): Boolean = {
    val coalesce = sink.coalesce.getOrElse(false)
    val targetPath = taskDesc.getTargetPath()
    logger.info(s"About to write resulting dataset to $targetPath")
    // Target Path exist only if a storage area has been defined at task or job level
    // To execute a task without writing to disk simply avoid the area at the job and task level

    val sinkPartition =
      sink.partition.getOrElse(Partition(sampling = None, attributes = taskDesc.partition))

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
        sinkPartition.attributes
      )

    val clusteredDFWriter = sink.clustering match {
      case None          => partitionedDFWriter
      case Some(columns) => partitionedDFWriter.sortBy(columns.head, columns.tail: _*)
    }

    val finalDataset = clusteredDFWriter
      .mode(taskDesc.getWrite().toSaveMode)
      .format(sink.format.getOrElse(settings.appConfig.defaultFormat))
      .options(sink.getOptions())
      .option("path", targetPath.toString)

    if (settings.appConfig.isHiveCompatible()) {
      val tableName = taskDesc.table
      val hiveDB = taskDesc.getHiveDB()
      val fullTableName = s"$hiveDB.$tableName"
      session.sql(s"create database if not exists $hiveDB")
      session.sql(s"use $hiveDB")
      if (taskDesc.getWrite().toSaveMode == SaveMode.Overwrite)
        session.sql(s"drop table if exists $tableName")
      finalDataset.saveAsTable(fullTableName)
      val tableTagPairs =
        Utils.extractTags(this.taskDesc.tags) + ("comment" -> taskDesc.comment.getOrElse(""))
      val tagsAsString = tableTagPairs.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
      session.sql(
        s"ALTER TABLE $fullTableName SET TBLPROPERTIES($tagsAsString)"
      )

      if (Utils.isRunningInDatabricks()) {
        taskDesc.attributesDesc.foreach { attrDesc =>
          session.sql(
            s"ALTER TABLE $tableName CHANGE COLUMN ${attrDesc}.name COMMENT '${attrDesc.comment}'"
          )
        }
      }
      analyze(fullTableName)
    } else {
      // TODO Handle SinkType.FS and SinkType to Hive in Sink section in the caller

      finalDataset.save()
      if (coalesce) {
        val extension =
          sink.extension.getOrElse(
            sink.format.getOrElse(settings.appConfig.defaultFormat)
          )
        val csvPath = storageHandler
          .list(targetPath, s".$extension", LocalDateTime.MIN, recursive = false)
          .head
        val finalPath = new Path(targetPath, targetPath.getName + s".$extension")
        storageHandler.move(csvPath, finalPath)
      }
    }
    true
  }

  private def registerFSViews(): List[String] = {
    val acceptedPath = DatasetArea.accepted(".")
    val domains =
      if (storageHandler.exists(acceptedPath)) storageHandler.listDirectories(acceptedPath)
      else Nil
    domains.flatMap { domain =>
      val domainName = domain.getName
      val tables = storageHandler.listDirectories(domain)
      tables.flatMap { table =>
        Try {
          val tableName = table.getName
          logger.info(s"registering view for $domainName.$tableName with path $table")
          val tableDF = session.read
            .format(settings.appConfig.defaultFormat)
            .load(table.toString)
          tableDF.createOrReplaceTempView(s"$tableName")
          tableName
        }.toOption
      }
    }
  }

  def runSpark(drop: Boolean): Try[(SparkJobResult, String)] = {
    val start = Timestamp.from(Instant.now())
    val res = Try {
      val localViews =
        if (
          sink.exists(_.isInstanceOf[FsSink]) && settings.appConfig.fileSystem.startsWith("file:")
        ) {
          // we are in local development mode
          registerFSViews()
        } else {
          Nil
        }

      val tableExists = session.catalog.tableExists(taskDesc.domain, taskDesc.table)

      val (preSql, sqlWithParameters, postSql) =
        buildAllSQLQueries(tableExists, localViews)
      preSql.foreach(req => session.sql(req))
      logger.info(s"""START COMPILE SQL $sqlWithParameters END COMPILE SQL""")
      logger.info(s"running sql request using ${taskDesc.getEngine()}")
      val dataframe = (taskDesc.sql, taskDesc.python) match {
        case (Some(sql), None) =>
          Some(loadSparkQuery(sqlWithParameters))
        case (None, Some(pythonFile)) =>
          runPySpark(pythonFile)
        case (None, None) =>
          throw new Exception(
            s"At least one SQL or Python command should be present in task ${taskDesc.name}"
          )
        case (Some(_), Some(_)) =>
          throw new Exception(
            s"Only one of 'sql' or 'python' attribute may be defined ${taskDesc.name}"
          )
      }
      dataframe match {
        case None =>
          (SparkJobResult(None), sqlWithParameters)
        case Some(dataframe) =>
          if (settings.appConfig.expectations.active) {
            new ExpectationJob(
              taskDesc.database,
              taskDesc.domain,
              taskDesc.table,
              taskDesc.expectations,
              storageHandler,
              schemaHandler,
              Some(Left(dataframe)),
              taskDesc.getEngine(),
              new SparkExpectationAssertionHandler(session)
            ).run()
          }

          postSql.foreach(req => session.sql(req))
          // Let us return the Dataframe so that it can be piped to another sink
          (SparkJobResult(Some(dataframe)), sqlWithParameters)
      }

    }
    val end = Timestamp.from(Instant.now())
    res match {
      case Success((jobResult, _)) =>
        val end = Timestamp.from(Instant.now())
        val jobResultCount = jobResult.dataframe match {
          case None => -1
          case Some(dataframe) =>
            dataframe.count()
        }
        logAuditSuccess(start, end, jobResultCount)
      case Failure(e) =>
        logAuditFailure(start, end, e)
    }
    res
  }

  private def runPySpark(pythonFile: Path): Option[DataFrame] = {
    // We first download locally all files because PythonRunner only support local filesystem
    val pyFiles =
      pythonFile +: settings.sparkConfig
        .getString("py-files")
        .split(",")
        .filter(_.nonEmpty)
        .map(x => new Path(x.trim))
    val directory = new Path(File.newTemporaryDirectory().pathAsString)
    logger.info(s"Python local directory is $directory")
    pyFiles.foreach { pyFile =>
      val pyName = pyFile.getName()
      storageHandler.copyToLocal(pyFile, new Path(directory, pyName))
    }
    val pythonParams = commandParameters.flatMap { case (name, value) =>
      List(s"""--$name""", s"""$value""")
    }.toArray

    PythonRunner.main(
      Array(
        new Path(directory, pythonFile.getName()).toString,
        pyFiles.mkString(",")
      ) ++ pythonParams
    )

    if (session.catalog.tableExists("SL_THIS"))
      Some(session.sqlContext.table("SL_THIS"))
    else
      None
  }

  private def loadSparkQuery(sqlWithParameters: String): DataFrame = {
    val connectionRef = sink.flatMap(_.connectionRef).getOrElse(settings.appConfig.connectionRef)
    val connection = settings.appConfig.connections(connectionRef)
    connection.getType() match {
      case ConnectionType.FS =>
        session.sql(sqlWithParameters)
      case _ =>
        session.read
          .format(connection.getSparkFormat())
          .option("query", sqlWithParameters)
          .options(connection.options)
          .load()
    }
  }

  private def logAudit(
    start: Timestamp,
    end: Timestamp,
    jobResultCount: Long,
    success: Boolean,
    message: String
  ): Unit = {
    val log = AuditLog(
      applicationId(),
      Some(this.name),
      this.taskDesc.domain,
      this.taskDesc.table,
      success,
      jobResultCount,
      -1,
      -1,
      start,
      end.getTime - start.getTime,
      message,
      Step.TRANSFORM.toString,
      taskDesc.getDatabase(),
      settings.appConfig.tenant
    )
    AuditLog.sink(optionalAuditSession, log)
  }

  private def logAuditSuccess(start: Timestamp, end: Timestamp, jobResultCount: Long): Unit =
    logAudit(start, end, jobResultCount, success = true, "success")

  private def logAuditFailure(start: Timestamp, end: Timestamp, e: Throwable): Unit =
    logAudit(start, end, -1, success = false, Utils.exceptionAsString(e))

  def dependencies(): List[String] = {
    val result = SQLUtils.extractRefsInSQL(parseJinja(taskDesc.getSql(), Map.empty))
    logger.info(s"$name has ${result.length} dependencies: ${result.mkString(",")}")
    result
  }

}
