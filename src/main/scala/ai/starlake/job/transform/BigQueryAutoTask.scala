package ai.starlake.job.transform

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.BigQueryTablesConfig
import ai.starlake.job.metrics.{BigQueryExpectationAssertionHandler, ExpectationJob}
import ai.starlake.job.sink.bigquery.{
  BigQueryJobBase,
  BigQueryJobResult,
  BigQueryLoadConfig,
  BigQueryNativeJob
}
import ai.starlake.schema.generator.ExtractBigQuerySchema
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.{JobResult, Utils}

import java.sql.Timestamp
import java.time.Instant
import scala.util.{Failure, Success, Try}

class BigQueryAutoTask(
  taskDesc: AutoTaskDesc,
  commandParameters: Map[String, String],
  interactive: Option[String],
  truncate: Boolean,
  resultPageSize: Int = 1
)(implicit settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends AutoTask(
      taskDesc,
      commandParameters,
      interactive,
      truncate,
      resultPageSize
    ) {

  val bqSink = taskDesc.sink
    .map(_.getSink())
    .getOrElse(BigQuerySink(connectionRef = Some(connectionRef)))
    .asInstanceOf[BigQuerySink]

  val tableId = BigQueryJobBase
    .extractProjectDatasetAndTable(taskDesc.database, taskDesc.domain, taskDesc.table)

  val fullTableName = BigQueryJobBase.getBqTableForNative(tableId)

  private def createBigQueryConfig(): BigQueryLoadConfig = {
    val bqSink =
      taskDesc.sink
        .map(_.getSink())
        .getOrElse(BigQuerySink(connectionRef = Some(connectionRef)))
        .asInstanceOf[BigQuerySink]

    tableId.toString
    BigQueryLoadConfig(
      connectionRef = Some(connectionRef),
      outputTableId = Some(tableId),
      createDisposition = createDisposition,
      writeDisposition = if (truncate) "WRITE_TRUNCATE" else writeDisposition,
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

  private def bqNativeJob(
    config: BigQueryLoadConfig,
    sql: String,
    jobTimeoutMs: Option[Long] = None
  ): BigQueryNativeJob = {
    val toUpperSql = sql.toUpperCase()
    val finalSql =
      if (toUpperSql.startsWith("WITH") || toUpperSql.startsWith("SELECT"))
        "(" + sql + ")"
      else
        sql
    new BigQueryNativeJob(config, finalSql, this.resultPageSize, jobTimeoutMs)
  }

  def runBQ(): Try[JobResult] = {
    val config = createBigQueryConfig()

    val start = Timestamp.from(Instant.now())
    if (truncate) {
      // nothing to do, config is created with write_truncate in that case
    }
    logger.info(s"running BQ Query start time $start")
    val jobRunner = bqNativeJob(config, "ignore sql", Some(settings.appConfig.shortJobTimeoutMs))
    val tableExists =
      jobRunner.tableExists(
        taskDesc.getDatabase(),
        taskDesc.domain,
        taskDesc.table
      )

    logger.info(s"running BQ Query with config $config")
    val (preSql, mainSql, postSql, mainIsSelect) =
      buildAllSQLQueries(tableExists, bqSink.timestamp, Some(fullTableName))
    logger.info(s"Config $config")
    // We add extra parenthesis required by BQ when using "WITH" keyword

    val presqlResult: List[Try[JobResult]] =
      preSql.map { sql =>
        logger.info(s"Running PreSQL BQ Query: $sql")
        bqNativeJob(config, sql).runInteractiveQuery()
      }
    presqlResult.foreach(Utils.logFailure(_, logger))

    logger.info(s"""START COMPILE SQL $mainSql END COMPILE SQL""")
    val jobResult: Try[JobResult] = interactive match {
      case None =>
        if (mainIsSelect)
          bqNativeJob(
            config,
            mainSql
          ).run()
        else
          bqNativeJob(
            config,
            mainSql
          ).runInteractiveQuery()
      case Some(_) =>
        bqNativeJob(
          config,
          mainSql
        ).runInteractiveQuery()
    }

    Utils.logFailure(jobResult, logger)

    // We execute the post statements even if the main statement failed
    // We may be doing some cleanup here.

    val postsqlResult: List[Try[JobResult]] =
      postSql.map { sql =>
        logger.info(s"Running PostSQL BQ Query: $sql")
        bqNativeJob(config, sql).runInteractiveQuery()
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
              Some(
                Right(jobRunner.getTableId(taskDesc.getDatabase(), taskDesc.domain, taskDesc.table))
              ),
              new BigQueryExpectationAssertionHandler(
                bqNativeJob(
                  config,
                  "",
                  taskDesc.taskTimeoutMs
                )
              )
            ).run()
          }
        }
        Try {
          val isTableInAuditDomain =
            taskDesc.domain == settings.appConfig.audit.getDomain()
          if (isTableInAuditDomain) {
            logger.info(
              s"Table ${taskDesc.domain}.${taskDesc.table} is in audit domain, skipping schema extraction"
            )
          } else {
            val config = BigQueryTablesConfig(tables = Map(taskDesc.domain -> List(taskDesc.table)))
            if (settings.appConfig.autoExportSchema)
              ExtractBigQuerySchema.extractAndSaveAsDomains(config)
          }
        } match {
          case Success(_) =>
            logger.info(
              s"Successfully wrote domain ${taskDesc.domain}.${taskDesc.table} to ${DatasetArea.external}"
            )
          case Failure(e) =>
            logger.warn(s"Failed to write domain ${taskDesc.domain} to ${DatasetArea.external}")
            logger.warn(Utils.exceptionAsString(e))
        }
        jobResult
      case _ =>
        val err = errors.reduce(_.initCause(_))
        val end = Timestamp.from(Instant.now())
        logAuditFailure(start, end, err)
        Failure(err)
    }
  }
  override def run(): Try[JobResult] = {
    runBQ()
  }
}
