package ai.starlake.job.transform

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.BigQueryTablesConfig
import ai.starlake.job.ingest.strategies.StrategiesBuilder
import ai.starlake.job.metrics.{BigQueryExpectationAssertionHandler, ExpectationJob}
import ai.starlake.job.sink.bigquery._
import ai.starlake.schema.generator.ExtractBigQuerySchema
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.conversion.BigQueryUtils
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import ai.starlake.utils.{JobResult, Utils}
import better.files.File
import com.google.cloud.bigquery.{
  Field,
  LegacySQLTypeName,
  Schema => BQSchema,
  StandardTableDefinition
}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.time.Instant
import scala.jdk.CollectionConverters.{asJavaIterableConverter, asScalaBufferConverter}
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

  private val bqSink = taskDesc.sink
    .map(_.getSink())
    .getOrElse(BigQuerySink(connectionRef = Some(sinkConnectionRef)))
    .asInstanceOf[BigQuerySink]

  private val tableId = BigQueryJobBase
    .extractProjectDatasetAndTable(taskDesc.getDatabase(), taskDesc.domain, taskDesc.table)

  val fullTableName: String = BigQueryJobBase.getBqTableForNative(tableId)

  override def tableExists: Boolean = {
    val tableExists =
      bqNativeJob(bigQuerySinkConfig, "ignore sql", Some(settings.appConfig.shortJobTimeoutMs))
        .tableExists(
          taskDesc.getDatabase(),
          taskDesc.domain,
          taskDesc.table
        )

    if (!tableExists && taskDesc._auditTableName.isDefined) {
      createAuditTable()
    } else
      tableExists
  }

  private def createAuditTable(): Boolean = {
    // Table not found and it is an table in the audit schema defined in the reference-connections.conf file  Try to create it.
    logger.info(s"Table ${taskDesc.table} not found in ${taskDesc.domain}")
    val entry = taskDesc._auditTableName.getOrElse(
      throw new Exception(
        s"audit table for output ${taskDesc.table} is not defined in engine $jdbcSinkEngineName"
      )
    )
    val scriptTemplate = jdbcSinkEngine.tables(entry).createSql

    val script = scriptTemplate.richFormat(
      Map("table" -> fullTableName, "writeFormat" -> settings.appConfig.defaultWriteFormat),
      Map.empty
    )
    runSqls(List(script)).forall(_.isSuccess)
  }

  private val bigQuerySinkConfig: BigQueryLoadConfig = {
    val bqSink =
      taskDesc.sink
        .map(_.getSink())
        .getOrElse(BigQuerySink(connectionRef = Some(sinkConnectionRef)))
        .asInstanceOf[BigQuerySink]

    BigQueryLoadConfig(
      connectionRef = Some(sinkConnectionRef),
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

  private def addSCD2Columns(config: BigQueryLoadConfig): Unit = {
    taskDesc.strategy match {
      case Some(strategyOptions) if strategyOptions.`type` == StrategyType.SCD2 =>
        config.outputTableId.foreach { tableId =>
          val scd2Cols = List(
            strategyOptions.start_ts.getOrElse(settings.appConfig.scd2StartTimestamp),
            strategyOptions.end_ts.getOrElse(settings.appConfig.scd2EndTimestamp)
          )
          val table = BigQueryJobBase.getBqTableForNative(tableId)
          val sql = scd2Cols
            .map { col =>
              s"ALTER TABLE $table ADD COLUMN IF NOT EXISTS $col TIMESTAMP OPTIONS(description='StarLake $col timestamp')"
            }
            .mkString(";\n")
          bqNativeJob(config, sql).runInteractiveQuery()
        }
      case None =>
    }
  }

  private def runSqls(sqls: List[String]): List[Try[BigQueryJobResult]] = {
    sqls.map { req =>
      logger.info(s"running sql request $req")
      bqNativeJob(bigQuerySinkConfig, req).runInteractiveQuery()
    }
  }
  def runOnDF(loadedDF: DataFrame): Try[JobResult] = {
    runBQ(Some(loadedDF))
  }

  private def runBQ(loadedDF: Option[DataFrame]): Try[JobResult] = {
    val config = bigQuerySinkConfig

    val start = Timestamp.from(Instant.now())
    if (truncate) {
      // nothing to do, config is created with write_truncate in that case
    }
    val mainSql =
      if (loadedDF.isEmpty && taskDesc.parseSQL.getOrElse(true)) {
        buildAllSQLQueries(None)
      } else {
        taskDesc.getSql()
      }

    val output = settings.appConfig.rootServe.map(File(_, "extension.log"))
    output.foreach(_.appendLine(s"$mainSql"))
    val jobResult: Try[JobResult] =
      interactive match {
        case None =>
          val source = loadedDF
            .map(df => Right(df))
            .getOrElse(Left(""))

          val presqlResult: List[Try[JobResult]] = runSqls(preSql)
          presqlResult.foreach(Utils.logFailure(_, logger))

          val jobResult: Try[JobResult] =
            loadedDF match {
              case Some(df) =>
                val bqLoadConfig =
                  BigQueryLoadConfig(
                    connectionRef = Some(sinkConnectionRef),
                    source = source,
                    outputTableId = Some(
                      BigQueryJobBase.extractProjectDatasetAndTable(
                        this.taskDesc.database,
                        this.taskDesc.domain,
                        this.taskDesc.table
                      )
                    ),
                    sourceFormat = settings.appConfig.defaultWriteFormat,
                    createDisposition = createDisposition,
                    writeDisposition = writeDisposition,
                    outputPartition = bqSink.timestamp,
                    outputClustering = bqSink.clustering.getOrElse(Nil),
                    days = bqSink.days,
                    requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
                    rls = this.taskDesc.rls,
                    acl = this.taskDesc.acl,
                    starlakeSchema = None,
                    // outputTableDesc = action.taskDesc.comment.getOrElse(""),
                    attributesDesc = this.taskDesc.attributesDesc,
                    outputDatabase = this.taskDesc.database
                  )
                val bqSparkJob =
                  new BigQuerySparkJob(bqLoadConfig, None, this.taskDesc.comment)
                val result = bqSparkJob.run()
                result.map { job =>
                  bqSparkJob.applyRLSAndCLS() match {
                    case Success(_) =>
                      job
                    case Failure(e) =>
                      throw e
                  }
                }
              case None =>
                val bqJob = bqNativeJob(
                  config,
                  mainSql
                )
                val result = bqJob.runInteractiveQuery()
                result.map { job =>
                  bqJob.applyRLSAndCLS() match {
                    case Success(_) =>
                      job
                    case Failure(e) =>
                      throw e
                  }
                }
            }

          jobResult.recover { case e =>
            Utils.logException(logger, e)
            throw e
          }

          val postsqlResult: List[Try[JobResult]] = runSqls(postSql)
          postsqlResult.foreach(Utils.logFailure(_, logger))

          val errors =
            (presqlResult ++ List(jobResult) ++ postsqlResult).map(_.failed).collect {
              case Success(e) =>
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
                  val config =
                    BigQueryTablesConfig(tables = Map(taskDesc.domain -> List(taskDesc.table)))
                  if (settings.appConfig.autoExportSchema)
                    ExtractBigQuerySchema.extractAndSaveAsDomains(config)
                }
              } match {
                case Success(_) =>
                  logger.info(
                    s"Successfully wrote domain ${taskDesc.domain}.${taskDesc.table} to ${DatasetArea.external}"
                  )
                case Failure(e) =>
                  logger.warn(
                    s"Failed to write domain ${taskDesc.domain} to ${DatasetArea.external}"
                  )
                  logger.warn(Utils.exceptionAsString(e))
              }
              jobResult
            case _ =>
              val err = errors.reduce(_.initCause(_))
              val end = Timestamp.from(Instant.now())
              logAuditFailure(start, end, err)
              Failure(err)
          }

        case Some(_) =>
          val res = bqNativeJob(
            config,
            mainSql
          ).runInteractiveQuery()
          addSCD2Columns(config)
          res
      }

    Utils.logFailure(jobResult, logger)

    // We execute the post statements even if the main statement failed
    // We may be doing some cleanup here.

  }
  override def run(): Try[JobResult] = {
    runBQ(None)
  }

  override def buildAllSQLQueries(sql: Option[String]): String = {
    assert(taskDesc.parseSQL.getOrElse(true))
    val sqlWithParameters = substituteRefTaskMainSQL(sql.getOrElse(taskDesc.getSql()))
    val columnNames = SQLUtils.extractColumnNames(sqlWithParameters)

    val mainSql = StrategiesBuilder(jdbcSinkEngine.strategyBuilder).buildSQLForStrategy(
      strategy,
      sqlWithParameters,
      fullTableName,
      columnNames,
      tableExists,
      truncate,
      isMaterializedView(),
      jdbcSinkEngine,
      sinkConfig
    )
    mainSql
  }

  def updateBigQueryTableSchema(incomingSparkSchema: StructType): Unit = {
    val bigqueryJob = bqNativeJob(bigQuerySinkConfig, "ignore sql")
    // When merging to BigQuery, load existing DF from BigQuery
    val bqTable = s"${taskDesc.domain}.${taskDesc.table}"
    val tableId =
      BigQueryJobBase.extractProjectDatasetAndTable(
        taskDesc.getDatabase(),
        taskDesc.domain,
        taskDesc.table
      )

    val tableExists = bigqueryJob.tableExists(tableId)

    val isSCD2 = strategy.`type` == StrategyType.SCD2
    def bqSchemaWithSCD2(incomingTableSchema: BQSchema): BQSchema = {
      if (
        isSCD2 && !incomingTableSchema.getFields.asScala.exists(
          _.getName == settings.appConfig.scd2StartTimestamp
        )
      ) {
        val startCol = Field
          .newBuilder(
            settings.appConfig.scd2StartTimestamp,
            LegacySQLTypeName.TIMESTAMP
          )
          .setMode(Field.Mode.NULLABLE)
          .build()
        val endCol = Field
          .newBuilder(
            settings.appConfig.scd2EndTimestamp,
            LegacySQLTypeName.TIMESTAMP
          )
          .setMode(Field.Mode.NULLABLE)
          .build()
        val allFields = incomingTableSchema.getFields.asScala.toList :+ startCol :+ endCol
        BQSchema.of(allFields.asJava)
      } else
        incomingTableSchema
    }

    if (tableExists) {
      val bqTable = bigqueryJob.getTable(tableId)
      bqTable
        .map { table =>
          // This will raise an exception if schemas are not compatible.
          val existingSchema = BigQuerySchemaConverters.toSpark(
            table.getDefinition[StandardTableDefinition].getSchema
          )

          // val incomingSchema = BigQueryUtils.normalizeSchema(schema.sparkSchemaWithoutIgnore(schemaHandler))
          // MergeUtils.computeCompatibleSchema(existingSchema, incomingSchema)
          val finalSparkSchema =
            BigQueryUtils.normalizeCompatibleSchema(incomingSparkSchema, existingSchema)
          logger.whenInfoEnabled {
            logger.info("Final target table schema")
            logger.info(finalSparkSchema.toString)
          }

          val newBqSchema = bqSchemaWithSCD2(BigQueryUtils.bqSchema(finalSparkSchema))
          val updatedTableDefinition =
            table.getDefinition[StandardTableDefinition].toBuilder.setSchema(newBqSchema).build()
          val updatedTable =
            table.toBuilder.setDefinition(updatedTableDefinition).build()
          updatedTable.update()
        }
    } else {
      val bqSchema = BigQueryUtils.bqSchema(incomingSparkSchema)
      val sink = sinkConfig.asInstanceOf[BigQuerySink]

      val partitionField = sink.timestamp.map { partitionField =>
        FieldPartitionInfo(partitionField, sink.days, sink.requirePartitionFilter.getOrElse(false))
      }
      val clusteringFields = sink.clustering.flatMap { fields =>
        Some(ClusteringInfo(fields.toList))
      }
      val newSchema = bqSchemaWithSCD2(bqSchema)
      val tableInfo = TableInfo(
        tableId,
        taskDesc.comment,
        Some(newSchema),
        partitionField,
        clusteringFields
      )
      bigqueryJob.getOrCreateTable(taskDesc._dbComment, tableInfo, None)
    }
  }
}
