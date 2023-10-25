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
import ai.starlake.schema.model.{AutoTaskDesc, BigQuerySink, Engine, Schema, Sink}
import ai.starlake.utils.{JobResult, Utils}
import org.apache.spark.sql.DataFrame

import java.sql.Timestamp
import java.time.Instant
import scala.util.{Failure, Success, Try}

class BigQueryAutoTask(
  taskDesc: AutoTaskDesc,
  commandParameters: Map[String, String],
  sink: Option[Sink],
  interactive: Option[String],
  database: Option[String],
  drop: Boolean,
  resultPageSize: Int = 1
)(implicit settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends AutoTask(
      taskDesc,
      commandParameters,
      sink,
      interactive,
      database,
      drop,
      resultPageSize
    ) {
  override def sink(maybeDataFrame: Option[DataFrame]): Boolean = {
    // Should be called for Spark only
    false
  }

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

  def runBQ(): Try[JobResult] = {
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
    val (preSql, mainSql, postSql) = buildAllSQLQueries(tableExists)
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
        Try {
          val config = BigQueryTablesConfig(tables = Map(taskDesc.domain -> List(taskDesc.table)))
          ExtractBigQuerySchema.extractAndSaveTables(config)
        } match {
          case Success(_) =>
            logger.info(
              s"Successfully wrote domain ${taskDesc.domain}.${taskDesc.table} to ${DatasetArea.external}"
            )
          case Failure(e) =>
            logger.warn(s"Failed to write domain ${taskDesc.domain} to ${DatasetArea.external}")
            throw e
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
