package ai.starlake.job.transform

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.core.utils.StringUtils
import ai.starlake.extract.{
  ExtractBigQuerySchema,
  ExtractSchemaCmd,
  ExtractSchemaConfig,
  TablesExtractConfig
}
import ai.starlake.job.metrics.{
  BigQueryExpectationAssertionHandler,
  ExpectationJob,
  ExpectationReport
}
import ai.starlake.job.sink.bigquery.*
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.conversion.BigQueryUtils
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import ai.starlake.utils.{JobResult, Utils}
import com.google.cloud.bigquery.{
  Field,
  LegacySQLTypeName,
  Schema as BQSchema,
  StandardTableDefinition
}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.time.Instant
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class BigQueryAutoTask(
  appId: Option[String],
  taskDesc: AutoTaskInfo,
  commandParameters: Map[String, String],
  interactive: Option[String],
  truncate: Boolean,
  test: Boolean,
  logExecution: Boolean,
  accessToken: Option[String] = None,
  resultPageSize: Int,
  resultPageNumber: Int,
  dryRun: Boolean,
  scheduledDate: Option[String]
)(implicit settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends AutoTask(
      appId,
      taskDesc,
      commandParameters,
      interactive,
      test,
      logExecution,
      truncate,
      resultPageSize,
      resultPageNumber,
      accessToken,
      None,
      scheduledDate
    ) {

  private lazy val bqSink = taskDesc.sink
    .map(_.getSink())
    .getOrElse(BigQuerySink(connectionRef = Some(sinkConnectionRef)))
    .asInstanceOf[BigQuerySink]

  private lazy val targetTableId = BigQueryJobBase
    .extractProjectDatasetAndTable(
      taskDesc.getDatabase(),
      taskDesc.domain,
      taskDesc.table,
      sinkOptions.get("projectId").orElse(settings.appConfig.getDefaultDatabase())
    )

  lazy val fullTableName: String = BigQueryJobBase.getBqTableForNative(targetTableId)

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

  def createAuditTable(): Boolean = {
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
    val thisSettings = settings
    val bqJob = new BigQueryJobBase {
      val settings: Settings = thisSettings
      override def cliConfig: BigQueryLoadConfig = new BigQueryLoadConfig(
        connectionRef = Some(taskDesc.getRunConnectionRef()(settings)),
        outputDatabase = None,
        accessToken = accessToken
      )
    }
    bqJob.getOrCreateDataset(None, Some(taskDesc.domain))
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
      outputTableId = Some(targetTableId),
      createDisposition = createDisposition,
      writeDisposition = if (truncate) "WRITE_TRUNCATE" else writeDisposition,
      outputPartition = bqSink.getPartitionColumn(),
      outputClustering = bqSink.clustering.getOrElse(Nil),
      days = bqSink.days,
      requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
      rls = taskDesc.rls,
      engine = Engine.BQ,
      acl = taskDesc.acl,
      materialization = taskDesc.sink
        .flatMap(_.getSink().asInstanceOf[BigQuerySink].materialization)
        .getOrElse(Materialization.TABLE),
      enableRefresh = bqSink.enableRefresh,
      refreshIntervalMs = bqSink.refreshIntervalMs,
      attributesDesc = taskDesc.attributes,
      outputTableDesc = taskDesc.comment,
      outputDatabase = taskDesc.getDatabase(),
      accessToken = accessToken
    )
  }

  private def bqNativeJob(
    config: BigQueryLoadConfig,
    sql: String,
    jobTimeoutMs: Option[Long] = None
  ): BigQueryNativeJob = {
    val toUpperSql = sql.toUpperCase()
    new BigQueryNativeJob(config, sql, this.resultPageSize, jobTimeoutMs)
  }

  private def runSqls(sqls: List[String]): List[Try[BigQueryJobResult]] = {
    sqls.map { req =>
      bqNativeJob(bigQuerySinkConfig, req).runInteractiveQuery()
    }
  }
  def runOnDF(loadedDF: DataFrame, sparkSchema: Option[StructType]): Try[JobResult] = {
    runBQ(Some(loadedDF), sparkSchema)
  }

  def runNative(): Try[JobResult] = {
    runBQ(None, None)
  }

  def build(): Map[String, Any] = {
    Map.empty
  }
  def runNative(sparkSchema: StructType): Try[JobResult] = {
    runBQ(None, Some(sparkSchema))
  }

  private def runBQ(
    loadedDF: Option[DataFrame],
    sparkSchema: Option[StructType]
  ): Try[JobResult] = {

    def mainSql(): String = {
      val targetSQL =
        if (loadedDF.isEmpty) {
          buildAllSQLQueries(None, tableExistsForcedValue = None, forceNative = true)
        } else {
          val mainSql: String = sqlSubst()
          mainSql
        }
      val trimmedSQL = targetSQL.trim()
      if (trimmedSQL.endsWith(";")) trimmedSQL.dropRight(1) else trimmedSQL
    }

    val config = bigQuerySinkConfig

    Utils.printOut(s"""
     |Table: $fullTableName
     |Connection: ${sinkConnectionRef}(${taskDesc.getSinkConnectionType()})
     |""".stripMargin)

    val start = Timestamp.from(Instant.now())
    if (truncate) {
      // nothing to do, config is created with write_truncate in that case
    }

    val jobResult: Try[JobResult] =
      interactive match {
        case None =>
          val jobResult: Try[JobResult] =
            loadedDF match {
              case Some(df) =>
                taskDesc.getSinkConfig().asInstanceOf[BigQuerySink].sharding match {
                  case Some(shardColumns) =>
                    val presqlResult: List[Try[JobResult]] = runSqls(preSql)
                    presqlResult.foreach(Utils.logFailure(_, logger))
                    val allResult =
                      df.select(shardColumns.head, shardColumns.tail: _*).distinct().collect().map {
                        row =>
                          val shard =
                            row.toSeq.map(Option(_).map(_.toString).getOrElse("null")).mkString("_")
                          logger.info(s"Processing shard $shard")
                          sparkSchema
                            .foreach(schema => updateBigQueryTableSchema(schema, Some(shard)))
                          val shardHead = shardColumns.head
                          val shardTail = shardColumns.tail
                          val conditions = shardTail
                            .map { shardColumn =>
                              df(shardColumn) === row.getAs(shardColumn)
                            }
                            .foldLeft(df(shardHead) === row.getAs(shardHead))(_ && _)
                          val result = saveDF(df.filter(conditions), Some(shard))
                          logger.info(s"Finished processing shard $shard with result $result")
                          result
                      }
                    allResult.find(_.isFailure).getOrElse(allResult.head)
                  case None =>
                    sparkSchema.foreach(schema => updateBigQueryTableSchema(schema))
                    saveDF(df, None)
                }
              case None =>
                taskDesc.getSinkConfig().asInstanceOf[BigQuerySink].sharding match {
                  case Some(shardColumns) =>
                    // TODO Check that we are in the second step of the load
                    val shardsQuery =
                      "SELECT DISTINCT " +
                      shardColumns.mkString(", ") +
                      " FROM (" + taskDesc.sql + ")"
                    val res = bqNativeJob(
                      config,
                      shardsQuery
                    ).runInteractiveQuery(dryRun = dryRun, pageSize = Some(1000))

                    val uniqueValues =
                      res.map { bqRes =>
                        val uniqueValues =
                          bqRes.tableResult
                            .map { rows =>
                              val values = rows.iterateAll().asScala.toList.map { row =>
                                row
                                  .iterator()
                                  .asScala
                                  .toList
                                  .map(x =>
                                    Option(x.getValue())
                                      .map(it =>
                                        StringUtils
                                          .replaceNonAlphanumericWithUnderscore(it.toString)
                                      )
                                      .getOrElse("null")
                                  )
                              }
                              values
                            }
                            .getOrElse(Nil)
                        uniqueValues
                      }
                    uniqueValues match {
                      case Success(values) =>
                        val allResult = values.map { shardValue =>
                          logger.info(s"Processing shard $shardValue")
                          val shardHead = shardColumns.head
                          val sharValueHead = shardValue.head
                          val shardValueTail = shardValue.tail
                          val shardTail = shardColumns.tail
                          val conditions = shardTail.zipWithIndex
                            .map { case (shardColumn, index) =>
                              s"$shardColumn = '${shardValueTail(index)}'"
                            }
                            .foldLeft(s"$shardHead = '$sharValueHead'")(_ + " AND " + _)
                          val shardSql = s"SELECT * FROM (${taskDesc.sql}) WHERE $conditions"
                          sparkSchema.foreach(schema =>
                            updateBigQueryTableSchema(
                              schema,
                              Some(shardValue.mkString("_"))
                            )
                          )
                          val resultApplyCLS = saveNative(config, shardSql)
                          logger.info(
                            s"Finished processing shard $shardValue with result $resultApplyCLS"
                          )
                          resultApplyCLS
                        }
                        allResult.find(_.isFailure).getOrElse(allResult.head)
                      case Failure(e) =>
                        Failure(e)
                    }
                  case None =>
                    sparkSchema.foreach(schema => updateBigQueryTableSchema(schema, None))
                    val allSql =
                      preSql.mkString(";\n") + mainSql() + ";\n" + postSql.mkString(";\n")
                    saveNative(config, allSql)
                }
            }

          jobResult.recover { case e =>
            Utils.logException(logger, e)
            throw e
          }

          val postsqlResult: List[Try[JobResult]] = runSqls(postSql)
          postsqlResult.foreach(Utils.logFailure(_, logger))

          jobResult match {
            case Success(_) =>
              jobResult map { jobResult =>
                val end = Timestamp.from(Instant.now())
                val jobResultCount =
                  jobResult.asInstanceOf[BigQueryJobResult].tableResult.map(_.getTotalRows)
                if (logExecution)
                  jobResultCount.foreach(logAuditSuccess(start, end, _, test))
                // We execute assertions only on success
                if (settings.appConfig.expectations.active && !taskDesc.isAuditTable()) {
                  runAndSinkExpectations()
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
                  if (settings.appConfig.autoExportSchema) {
                    val config = ExtractSchemaConfig(
                      external = true,
                      outputDir = Some(DatasetArea.external.toString),
                      tables = s"${taskDesc.domain}.${taskDesc.table}" :: Nil,
                      connectionRef = Some(sinkConnectionRef),
                      accessToken = accessToken
                    )
                    ExtractSchemaCmd.run(config, schemaHandler)
                  }
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
            case Failure(err) =>
              val end = Timestamp.from(Instant.now())
              logAuditFailure(start, end, err, test)
              Failure(err)
          }

        case Some(_) =>
          // interactive query, we limit the number of rows to maxInteractiveRecords
          val limitSql = limitQuery(mainSql(), resultPageSize, resultPageNumber)
          val res = bqNativeJob(
            config,
            limitSql
          ).runInteractiveQuery(dryRun = dryRun, pageSize = Some(1000))

          res.foreach { _ =>
            if (settings.appConfig.autoExportSchema) {
              Try {
                if (!taskDesc.getSql().startsWith("DESCRIBE ")) {
                  SQLUtils.extractTableNames(sqlSubst()).foreach { domainAndTableName =>
                    val components =
                      SQLUtils.unquoteAgressive(domainAndTableName.split("\\.").toList)
                    if (components.size == 2) {
                      val domainName = components(0)
                      val tableName = components(1)
                      val slFile =
                        new Path(new Path(DatasetArea.external, domainName), s"$tableName.sl.yml")
                      if (!storageHandler.exists(slFile)) {
                        val config =
                          TablesExtractConfig(tables = Map(domainName -> List(tableName)))
                        ExtractBigQuerySchema.extractAndSaveToExternal(config, schemaHandler)
                      }
                    }
                  }
                }
              } match {
                case Success(_) =>
                case Failure(e) =>
                  logger.warn(Utils.exceptionAsString(e))
              }
            }
          }
          res
      }

    Utils.logFailure(jobResult, logger)

    // We execute the post statements even if the main statement failed
    // We may be doing some cleanup here.

  }

  def runAndSinkExpectations(): Try[JobResult] = {
    new ExpectationJob(
      Option(applicationId()),
      taskDesc.database,
      taskDesc.domain,
      taskDesc.table,
      taskDesc.expectations,
      storageHandler,
      schemaHandler,
      new BigQueryExpectationAssertionHandler(
        bqNativeJob(
          bigQuerySinkConfig,
          "",
          taskDesc.taskTimeoutMs
        )
      ),
      false
    ).run()
  }

  def runExpectations(): List[ExpectationReport] = {
    new ExpectationJob(
      Option(applicationId()),
      taskDesc.database,
      taskDesc.domain,
      taskDesc.table,
      taskDesc.expectations,
      storageHandler,
      schemaHandler,
      new BigQueryExpectationAssertionHandler(
        bqNativeJob(
          bigQuerySinkConfig,
          "",
          taskDesc.taskTimeoutMs
        )
      ),
      true
    ).runExpectations()
  }

  private def sqlSubst(): String = {
    val sql = taskDesc.getSql()
    val mainSql = schemaHandler.substituteRefTaskMainSQL(
      sql,
      taskDesc.getRunConnection(),
      allVars
    )
    mainSql
  }

  private def saveNative(config: BigQueryLoadConfig, mainSql: String) = {
    val bqJob = bqNativeJob(
      config,
      mainSql
    )
    val result = bqJob.runInteractiveQuery(dryRun = dryRun)
    result.map { job =>
      bqJob.applyRLSAndCLS() match {
        case Success(_) =>
          job
        case Failure(e) =>
          throw e
      }
    }
  }

  private def saveDF(source: DataFrame, shard: Option[String]): Try[JobResult] = {
    val bqLoadConfig =
      BigQueryLoadConfig(
        connectionRef = Some(sinkConnectionRef),
        source = Right(source),
        outputTableId = Some(
          BigQueryJobBase.extractProjectDatasetAndTable(
            this.taskDesc.getDatabase(),
            this.taskDesc.domain,
            this.taskDesc.table + shard
              .map("_" + StringUtils.replaceNonAlphanumericWithUnderscore(_))
              .getOrElse(""),
            sinkOptions.get("projectId").orElse(settings.appConfig.getDefaultDatabase())
          )
        ),
        sourceFormat = settings.appConfig.defaultWriteFormat,
        createDisposition = createDisposition,
        writeDisposition = writeDisposition,
        outputPartition = bqSink.getPartitionColumn(),
        outputClustering = bqSink.clustering.getOrElse(Nil),
        days = bqSink.days,
        requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
        rls = this.taskDesc.rls,
        acl = this.taskDesc.acl,
        starlakeSchema = None,
        // outputTableDesc = action.taskDesc.comment.getOrElse(""),
        attributesDesc = this.taskDesc.attributes,
        outputDatabase = this.taskDesc.database,
        accessToken = accessToken
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
  }

  override def run(): Try[JobResult] = {
    runNative()
  }

  private def bqSchemaWithSCD2(incomingTableSchema: BQSchema): BQSchema = {
    val isSCD2 = writeStrategy.getEffectiveType() == WriteStrategyType.SCD2
    if (
      isSCD2 && !incomingTableSchema.getFields.asScala.exists(
        _.getName().toLowerCase() == settings.appConfig.scd2StartTimestamp.toLowerCase()
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

  def buildACLQueries(): List[String] = {
    val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
      taskDesc.getDatabase(),
      taskDesc.domain,
      taskDesc.table,
      sinkOptions.get("projectId").orElse(settings.appConfig.getDefaultDatabase())
    )
    BigQueryJobBase.buildACLQueries(tableId, taskDesc.acl)
  }

  override def buildConnection(): Map[String, String] = {
    val result = sinkConnection.asMap()
    taskDesc.getDatabase() match {
      case Some(db) =>
        result.updated("projectId", db)
      case None =>
        sinkConnection.options.get("projectId") match {
          case Some(db) =>
            result.updated("projectId", db)
          case None =>
            result
        }
    }
  }

  override def buildRLSQueries(): List[String] = {
    def revokeAllPrivileges(): String = {
      val outputTable = BigQueryJobBase.getBqTableForNative(targetTableId)
      s"DROP ALL ROW ACCESS POLICIES ON $outputTable"
    }

    /** Grant privileges to the users and groups defined in the schema
      * @param rlsRetrieved
      * @return
      */
    def grantPrivileges(rlsRetrieved: RowLevelSecurity): String = {
      val grants = rlsRetrieved.grantees().map {
        case (UserType.SA, u) =>
          s"serviceAccount:$u"
        case (userOrGroupOrDomainType, userOrGroupOrDomainName) =>
          s"${userOrGroupOrDomainType.toString.toLowerCase}:$userOrGroupOrDomainName"
      }

      val name = rlsRetrieved.name
      val filter = rlsRetrieved.predicate
      val outputTable = BigQueryJobBase.getBqTableForNative(targetTableId)
      s"""
               | CREATE ROW ACCESS POLICY
               |  $name
               | ON
               |  $outputTable
               | GRANT TO
               |  (${grants.mkString("\"", "\",\"", "\"")})
               | FILTER USING
               |  ($filter)
               |""".stripMargin
    }
    val rlsCreateStatements = taskDesc.rls.map { rlsRetrieved =>
      logger.info(s"Building security statement $rlsRetrieved")
      val rlsCreateStatement = grantPrivileges(rlsRetrieved)
      logger.info(s"An access policy will be created using $rlsCreateStatement")
      rlsCreateStatement
    }
    val rlsDeleteStatement = taskDesc.rls.map(_ => revokeAllPrivileges())
    rlsDeleteStatement ++ rlsCreateStatements

  }

  def updateBigQueryTableSchema(
    incomingSparkSchema: StructType,
    sharding: Option[String] = None
  ): Unit = {
    val bigqueryJob = bqNativeJob(bigQuerySinkConfig, "ignore sql")
    val tableId =
      BigQueryJobBase.extractProjectDatasetAndTable(
        taskDesc.getDatabase(),
        taskDesc.domain,
        taskDesc.table + sharding.map("_" + _).getOrElse(""),
        sinkOptions.get("projectId").orElse(settings.appConfig.getDefaultDatabase())
      )

    val tableExists = bigqueryJob.tableExists(tableId)

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

      val partitionField = sink.getPartitionColumn().map { partitionField =>
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
      val targetTableId = sharding.map(_ => tableId)
      bigqueryJob.getOrCreateTable(taskDesc._dbComment, tableInfo, None, targetTableId)
    }
  }
}
