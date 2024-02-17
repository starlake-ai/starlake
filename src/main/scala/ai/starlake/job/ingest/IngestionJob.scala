package ai.starlake.job.ingest

import ai.starlake.config.{CometColumns, DatasetArea, Settings, StorageArea}
import ai.starlake.exceptions.{DisallowRejectRecordException, NullValueFoundException}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.metrics._
import ai.starlake.job.sink.bigquery._
import ai.starlake.job.sink.es.{ESLoadConfig, ESLoadJob}
import ai.starlake.job.sink.jdbc.{sparkJdbcLoader, JdbcConnectionLoadCmd, JdbcConnectionLoadConfig}
import ai.starlake.job.transform.{AutoTask, SparkAutoTask}
import ai.starlake.job.validator.{GenericRowValidator, ValidationResult}
import ai.starlake.privacy.PrivacyEngine
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.Rejection.{ColInfo, ColResult}
import ai.starlake.schema.model.Trim.{BOTH, LEFT, NONE, RIGHT}
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter._
import ai.starlake.utils._
import ai.starlake.utils.conversion.BigQueryUtils
import ai.starlake.utils.kafka.KafkaClient
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.{
  Field,
  LegacySQLTypeName,
  Schema => BQSchema,
  StandardTableDefinition,
  Table,
  TableId
}
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{call_udf, col, expr, struct}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql._

import java.nio.charset.Charset
import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}
import scala.annotation.nowarn
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try, Using}

trait IngestionJob extends SparkJob {

  private def loadGenericValidator(validatorClass: String): GenericRowValidator = {
    val validatorClassName = loader.toLowerCase() match {
      case "spark" => validatorClass
      case "native" =>
        logger.warn(s"Unexpected '$loader' loader !!!")
        validatorClass
      case _ =>
        throw new Exception(s"Unexpected '$loader' loader !!!")
    }
    Utils.loadInstance[GenericRowValidator](validatorClassName)
  }

  protected lazy val treeRowValidator: GenericRowValidator = {
    loadGenericValidator(settings.appConfig.treeValidatorClass)
  }

  protected lazy val flatRowValidator: GenericRowValidator =
    loadGenericValidator(settings.appConfig.rowValidatorClass)

  def domain: Domain

  def schema: Schema

  def storageHandler: StorageHandler

  def schemaHandler: SchemaHandler

  def types: List[Type]

  def path: List[Path]

  def options: Map[String, String]

  val now: Timestamp = java.sql.Timestamp.from(Instant.now)

  /** Merged metadata
    */
  lazy val mergedMetadata: Metadata = schema.mergedMetadata(domain.metadata)
  lazy val loader: String = mergedMetadata.loader.getOrElse(settings.appConfig.loader)

  /** ingestion algorithm
    *
    * @param dataset
    */
  protected def ingest(dataset: DataFrame): (Dataset[String], Dataset[Row], Long)

  protected def reorderTypes(orderedAttributes: List[Attribute]): (List[Type], StructType) = {
    val typeMap: Map[String, Type] = types.map(tpe => tpe.name -> tpe).toMap
    val (tpes, sparkFields) = orderedAttributes.map { attribute =>
      val tpe = typeMap(attribute.`type`)
      (tpe, tpe.sparkType(attribute.name, !attribute.required, attribute.comment))
    }.unzip
    (tpes, StructType(sparkFields))
  }

  /** @param datasetHeaders
    *   : Headers found in the dataset
    * @param schemaHeaders
    *   : Headers defined in the schema
    * @return
    *   two lists : One with thecolumns present in the schema and the dataset and another with the
    *   headers present in the dataset only
    */
  protected def intersectHeaders(
    datasetHeaders: List[String],
    schemaHeaders: List[String]
  ): (List[String], List[String]) = {
    datasetHeaders.partition(schemaHeaders.contains)
  }

  private def getWriteMode(): WriteMode =
    schema.merge
      .map(_ => WriteMode.OVERWRITE)
      .getOrElse(mergedMetadata.getWrite())
  def getConnectionType(): ConnectionType = {
    val connectionRef =
      mergedMetadata.getSink().connectionRef.getOrElse(settings.appConfig.connectionRef)
    settings.appConfig.connections(connectionRef).getType()
  }

  private def csvOutput(): Boolean = {
    mergedMetadata.getSink() match {
      case fsSink: FsSink =>
        val format = fsSink.format.getOrElse("")
        (settings.appConfig.csvOutput || format == "csv") &&
        !settings.appConfig.grouped && fsSink.partition.isEmpty && path.nonEmpty
      case _ =>
        false
    }
  }

  /** This function is called only if csvOutput is true This means we are sure that sink is an
    * FsSink
    *
    * @return
    */
  private def csvOutputExtension(): String = {

    if (settings.appConfig.csvOutputExt.nonEmpty)
      settings.appConfig.csvOutputExt
    else {
      mergedMetadata.sink.flatMap(_.getSink().asInstanceOf[FsSink].extension).getOrElse("")
    }
  }

  private def extractHiveTableAcl(): List[String] = {

    if (settings.appConfig.isHiveCompatible()) {
      val fullTableName = schemaHandler.getFullTableName(domain, schema)
      schema.acl.flatMap { ace =>
        if (Utils.isRunningInDatabricks()) {
          /*
        GRANT
          privilege_type [, privilege_type ] ...
          ON (CATALOG | DATABASE <database-name> | TABLE <table-name> | VIEW <view-name> | FUNCTION <function-name> | ANONYMOUS FUNCTION | ANY FILE)
          TO principal

        privilege_type
          : SELECT | CREATE | MODIFY | READ_METADATA | CREATE_NAMED_FUNCTION | ALL PRIVILEGES
           */
          ace.asDatabricksSql(fullTableName)
        } else { // Hive
          ace.asHiveSql(fullTableName)
        }
      }
    } else {
      Nil
    }
  }

  def applyHiveTableAcl(forceApply: Boolean = false): Try[Unit] =
    Try {
      if (forceApply || settings.appConfig.accessPolicies.apply) {
        val sqls = extractHiveTableAcl()
        sqls.foreach { sql =>
          logger.info(sql)
          session.sql(sql)
        }
      }
    }

  private def extractJdbcAcl(): List[String] = {
    val fullTableName = schemaHandler.getFullTableName(domain, schema)
    schema.acl.flatMap { ace =>
      /*
        https://docs.snowflake.com/en/sql-reference/sql/grant-privilege
        https://hevodata.com/learn/snowflake-grant-role-to-user/
       */
      ace.asJdbcSql(fullTableName)
    }
  }

  def applyJdbcAcl(connection: Settings.Connection, forceApply: Boolean = false): Try[Unit] =
    AccessControlEntry.applyJdbcAcl(connection, extractJdbcAcl(), forceApply)

  private def runPreSql(): Unit = {
    val bqConfig = BigQueryLoadConfig(
      connectionRef = Some(mergedMetadata.getConnectionRef()),
      outputDatabase = schemaHandler.getDatabase(domain)
    )

    def bqNativeJob(sql: String)(implicit settings: Settings) = {
      new BigQueryNativeJob(bqConfig, sql)
    }

    schema.presql.foreach { sql =>
      val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
      mergedMetadata.getEngine() match {
        case Engine.BQ =>
          bqNativeJob(compiledSql).runInteractiveQuery()
        case _ =>
          session.sql(compiledSql)
      }
    }
  }

  private def runPostSQL(mergedDF: DataFrame): DataFrame = {
    schema.postsql.foldLeft(mergedDF) { (df, query) =>
      df.createOrReplaceTempView("SL_THIS")
      df.sparkSession.sql(query.richFormat(schemaHandler.activeEnvVars(), options))
    }
  }

  private def selectLoadEngine(): Engine = {
    val nativeCandidate: Boolean = isNativeCandidate()

    val engine = mergedMetadata.getEngine()

    if (nativeCandidate && engine == Engine.BQ) {
      logger.info("Using BQ as ingestion engine")
      Engine.BQ
    } else if (engine == Engine.JDBC) {
      logger.info("Using JDBC as ingestion engine")
      Engine.JDBC
    } else {
      logger.info("Using Spark as ingestion engine")
      Engine.SPARK
    }
  }

  private def isNativeCandidate(): Boolean = {
    val csvOrJsonLines =
      !mergedMetadata.isArray() && Set(Format.DSV, Format.JSON, Format.SIMPLE_JSON).contains(
        mergedMetadata.getFormat()
      )

    val nativeValidator =
      mergedMetadata.loader.getOrElse(settings.appConfig.loader).toLowerCase().equals("native")
    // https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
    csvOrJsonLines && nativeValidator
  }

  def run(): Try[JobResult] = {
    selectLoadEngine() match {
      case Engine.BQ =>
        runBQNative().map(_.jobResult)
      case Engine.SPARK =>
        runSpark()
      case Engine.JDBC =>
        runJDBCNative()
      case _ =>
        throw new Exception("should never happen")
    }
  }
  ///////////////////////////////////////////////////////////////////////////
  /////// JDBC ENGINE ONLY (SPARK SECTION BELOW) ////////////////////////////
  ///////////////////////////////////////////////////////////////////////////
  def runJDBCNative(): Try[JobResult] = {
    session.sparkContext.setLocalProperty(
      "spark.scheduler.pool",
      settings.appConfig.sparkScheduling.poolName
    )
    val jobResult = domain.checkValidity(schemaHandler) match {
      case Left(errors) =>
        val errs = errors.map(_.toString()).reduce { (errs, err) =>
          errs + "\n" + err
        }
        Failure(throw new Exception(s"-- Domain $name --\n" + errs))
      case Right(_) =>
        val start = Timestamp.from(Instant.now())
        runPreSql()
        val dataset = loadDataSet(true)
        dataset match {
          case Success(dataset) =>
            val jdbcSink = mergedMetadata.getSink().asInstanceOf[JdbcSink]
            val targetTable = domain.finalName + "." + schema.finalName
            val twoSteps = requireTwoSteps(schema, jdbcSink)
            val connection = mergedMetadata.getConnection()
            val hasMergeKey = schema.merge.exists(_.key.nonEmpty)
            val firstStepTempTable =
              if (!twoSteps) {
                targetTable
              } else {
                domain.finalName + "." + SQLUtils.temporaryTableName(schema.finalName)
              }
            val result = Try {

              val (createDisposition: String, writeDisposition: String) = Utils.getDBDisposition(
                mergedMetadata.getWrite(),
                hasMergeKey,
                isJDBC = true
              )
              val datasetWithRenamedAttributes = dfWithAttributesRenamed(dataset)
              val jdbcConnectionLoadConfig = JdbcConnectionLoadConfig(
                sourceFile = Right(datasetWithRenamedAttributes),
                outputDomainAndTableName = firstStepTempTable,
                createDisposition = CreateDisposition.valueOf(createDisposition),
                writeDisposition = WriteDisposition.valueOf(writeDisposition),
                format = "jdbc",
                options = connection.options
              )
              // Create table and load the data using Spark
              val jdbcJob = new sparkJdbcLoader(jdbcConnectionLoadConfig)
              val firstStepResult = jdbcJob.run()

              if (twoSteps) { // We need to apply transform, filter, merge, ...
                firstStepResult match {
                  case Success(loadFileResult) =>
                    logger.info(s"First step result: $loadFileResult")
                    JdbcDbUtils.withJDBCConnection(connection) { conn =>
                      val expectationsResult = runExpectations(firstStepTempTable, conn)
                      val keepGoing =
                        expectationsResult.isSuccess || !settings.appConfig.expectations.failOnError
                      if (keepGoing) {
                        // is it a new table ?
                        val targetTableExists =
                          JdbcDbUtils.tableExists(conn, connection.jdbcUrl, targetTable)

                        if (targetTableExists) {
                          // update target table schema if needed
                          logger.info(
                            s"Schema ${domain.finalName}"
                          )
                          val existingSchema =
                            SparkUtils.getSchemaOption(conn, connection.options, targetTable)
                          val incomingSchema =
                            datasetWithRenamedAttributes
                              .drop(CometColumns.cometInputFileNameColumn)
                              .schema
                          val addedSchema =
                            SparkUtils.added(
                              incomingSchema,
                              existingSchema.getOrElse(incomingSchema)
                            )
                          val deletedSchema =
                            SparkUtils.dropped(
                              incomingSchema,
                              existingSchema.getOrElse(incomingSchema)
                            )
                          val alterTableDropColumns =
                            SparkUtils.alterTableDropColumnsString(deletedSchema, targetTable)
                          if (alterTableDropColumns.nonEmpty) {
                            logger.info(
                              s"alter table $targetTable with ${alterTableDropColumns.size} columns to drop"
                            )
                            logger.debug(s"alter table ${alterTableDropColumns.mkString("\n")}")
                          }

                          val alterTableAddColumns =
                            SparkUtils.alterTableAddColumnsString(addedSchema, targetTable)
                          if (alterTableAddColumns.nonEmpty) {
                            logger.info(
                              s"alter table $targetTable with ${alterTableAddColumns.size} columns to add"
                            )
                            logger.debug(s"alter table ${alterTableAddColumns.mkString("\n")}")
                          }

                          alterTableDropColumns.foreach(JdbcDbUtils.executeAlterTable(_, conn))
                          alterTableAddColumns.foreach(JdbcDbUtils.executeAlterTable(_, conn))
                        }
                        // At this point if the table exists, it has the same schema as the dataframe

                        val secondStepResult =
                          applyJdbcSecondStep(
                            firstStepTempTable,
                            schema,
                            targetTableExists,
                            hasMergeKey
                          )
                        secondStepResult

                      } else {
                        expectationsResult
                      }
                    }

                  case Failure(exception) =>
                    Failure(exception)
                }
              } else {
                firstStepResult
              }
            } match {
              case Failure(exception) =>
                logLoadFailureInAudit(start, exception)
              case Success(result) =>
                result
            }
            if (twoSteps) {
              JdbcDbUtils.withJDBCConnection(connection) { conn =>
                JdbcDbUtils.dropTable(firstStepTempTable, conn)
              }
            }
            result
          case Failure(exception) =>
            logLoadFailureInAudit(start, exception)
        }
    }
    // After each ingestionjob we explicitely clear the spark cache
    session.catalog.clearCache()
    jobResult

  }

  private def applyJdbcSecondStep(
    firstStepTempTableName: String,
    starlakeSchema: Schema,
    targetTableExists: Boolean,
    hasMergeKey: Boolean
  ): Try[JobResult] = {
    val engineName = mergedMetadata.getSink().getConnection().getJdbcEngineName()
    val fullTableName = s"${domain.finalName}.${schema.finalName}"

    val quote = settings.appConfig.jdbcEngines(engineName.toString.toLowerCase()).quote
    // postgres does not support merge / create __or replace__ table. We need to do it by hand
    val (outputTableName, fullOutputTableName) = (schema.finalName, fullTableName)

    val tempTable = firstStepTempTableName
    val targetTable = fullOutputTableName

    // Even if merge is able to handle data deletion, in order to have same behavior with spark
    // we require user to set dynamic partition overwrite
    // we have sql as optional because in dynamic partition overwrite mode, if no partition exists, we do nothing
    val sqlMerge = starlakeSchema.merge match {
      case Some(_: MergeOptions) =>
        handleJdbcNativeMergeCases(
          starlakeSchema,
          targetTable,
          tempTable,
          targetTableExists
        )
      case None =>
        handleJdbcNativeNoMergeCases(
          starlakeSchema,
          tempTable
        )
    }
    val sqlMerges = sqlMerge.map(_.replace("`", quote).split(";\n"))
    sqlMerges match {
      case Success(sqlMerges) =>
        val (presql, mainSql) =
          if (sqlMerges.length == 1) {
            (Nil, sqlMerges.head)
          } else {
            (sqlMerges.dropRight(1).toList, sqlMerges.last)
          }
        val taskDesc = AutoTaskDesc(
          name = targetTable,
          presql = presql,
          sql = Some(mainSql),
          database = schemaHandler.getDatabase(domain),
          domain = domain.finalName,
          table = schema.finalName,
          write = Some(mergedMetadata.getWrite()),
          sink = mergedMetadata.sink,
          acl = schema.acl,
          comment = schema.comment,
          tags = schema.tags,
          parseSQL = Some(false)
        )
        val jobResult = AutoTask
          .task(taskDesc, Map.empty, None, truncate = false)(
            settings,
            storageHandler,
            schemaHandler
          )
          .run()
        jobResult
      case Failure(exception) =>
        Failure(exception)
    }
  }

  private def handleJdbcNativeMergeCases(
    starlakeSchema: Schema,
    targetTable: String,
    tempTable: String,
    targetTableExists: Boolean
  ): Try[String] = {
    Success(
      starlakeSchema
        .buildJDBCSqlMergeOnLoad(
          tempTable,
          targetTable,
          targetTableExists,
          mergedMetadata.getSink().getConnection().getJdbcEngineName()
        )
    )
  }

  private def handleJdbcNativeNoMergeCases(
    schema: Schema,
    tempTable: String
  ): Try[String] = {
    Success(schema.buildSqlSelectOnLoad(tempTable, None))
  }

  private def requireTwoSteps(schema: Schema, sink: JdbcSink): Boolean = {
    // renamed attribute can be loaded directly so it's not in the condition
    schema
      .hasTransformOrIgnoreOrScriptColumns() ||
    schema.merge.nonEmpty ||
    schema.filter.nonEmpty ||
    settings.appConfig.archiveTable
  }
  ///////////////////////////////////////////////////////////////////////////
  /////// BQ ENGINE ONLY (SPARK SECTION BELOW) //////////////////////////////
  ///////////////////////////////////////////////////////////////////////////

  private def requireTwoSteps(schema: Schema, sink: BigQuerySink): Boolean = {
    // renamed attribute can be loaded directly so it's not in the condition
    schema
      .hasTransformOrIgnoreOrScriptColumns() ||
    schema.merge.nonEmpty ||
    schema.filter.nonEmpty ||
    sink.dynamicPartitionOverwrite.getOrElse(false) ||
    settings.appConfig.archiveTable
  }

  def runBQNative(): Try[NativeBqLoadInfo] = {
    val start = Timestamp.from(Instant.now())
    Try {
      val effectiveSchema: Schema = computeEffectiveInputSchema()
      val (createDisposition: String, writeDisposition: String) = Utils.getDBDisposition(
        mergedMetadata.getWrite(),
        effectiveSchema.merge.exists(_.key.nonEmpty),
        isJDBC = false
      )
      val bqSink = mergedMetadata.getSink().asInstanceOf[BigQuerySink]
      val schemaWithMergedMetadata = effectiveSchema.copy(metadata = Some(mergedMetadata))

      val targetTableId =
        BigQueryJobBase
          .extractProjectDatasetAndTable(
            schemaHandler
              .getDatabase(domain),
            domain.finalName,
            effectiveSchema.finalName
          )

      val targetConfig = buildCommonNativeBQLoadConfig(
        createDisposition,
        writeDisposition,
        bqSink,
        schemaWithMergedMetadata
      ).copy(
        outputTableId = Some(targetTableId),
        days = bqSink.days,
        outputPartition = bqSink.timestamp,
        outputClustering = bqSink.clustering.getOrElse(Nil),
        requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
        rls = effectiveSchema.rls
      )
      if (requireTwoSteps(effectiveSchema, bqSink)) {
        val firstStepTempTable = BigQueryJobBase.extractProjectDatasetAndTable(
          schemaHandler.getDatabase(domain),
          domain.finalName,
          SQLUtils.temporaryTableName(effectiveSchema.finalName)
        )
        val firstStepConfig = buildCommonNativeBQLoadConfig(
          createDisposition,
          writeDisposition,
          bqSink,
          schemaWithMergedMetadata
        ).copy(
          outputTableId = Some(firstStepTempTable),
          outputTableDesc = Some("Temporary table created during data ingestion."),
          days = Some(1)
        )
        val firstStepBigqueryJob = new BigQueryNativeJob(firstStepConfig, "")
        val firstStepTableInfo = firstStepBigqueryJob.getTableInfo(
          firstStepTempTable,
          _.bqSchemaWithIgnoreAndScript(schemaHandler)
        )
        // TODO What if type is changed by transform ? we need to use safe_cast to have the same behavior as in SPARK.
        val firstStepResult =
          firstStepBigqueryJob.loadPathsToBQ(firstStepTableInfo)

        val output: Try[NativeBqLoadInfo] = firstStepResult match {
          case Success(loadFileResult) =>
            logger.info(s"First step result: $loadFileResult")
            val domainAndTableName = BigQueryUtils.tableIdToString(firstStepTempTable)
            val expectationsResult = runExpectations(domainAndTableName, firstStepBigqueryJob)
            val keepGoing =
              expectationsResult.isSuccess || !settings.appConfig.expectations.failOnError
            if (keepGoing) {
              val targetTableSchema = effectiveSchema.bqSchemaWithoutIgnore(schemaHandler)
              val targetBigqueryJob = new BigQueryNativeJob(targetConfig, "")
              val secondStepResult =
                applyBigQuerySecondStep(
                  targetBigqueryJob,
                  firstStepTempTable,
                  targetTableSchema,
                  schema
                )

              def updateRejectedCount(nullCountValues: Long) = {
                firstStepResult.map(r =>
                  r.copy(
                    totalAcceptedRows = r.totalAcceptedRows - nullCountValues,
                    totalRejectedRows = r.totalRejectedRows + nullCountValues
                  )
                )
              }
              secondStepResult
                .flatMap { case (_, nullCountValues) =>
                  updateRejectedCount(nullCountValues)
                } // keep loading stats
                .recoverWith { case ex: NullValueFoundException =>
                  updateRejectedCount(ex.nbRecord)
                }
            } else {
              expectationsResult.asInstanceOf[Try[NativeBqLoadInfo]]
            }
          case res @ Failure(_) =>
            res
        }
        if (settings.appConfig.archiveTable) {
          val (
            archiveDatabaseName: scala.Option[String],
            archiveDomainName: String,
            archiveTableName: String
          ) = getArchiveTableComponents()

          val targetTable = OutputRef(
            firstStepTempTable.getProject(),
            firstStepTempTable.getDataset(),
            firstStepTempTable.getTable()
          ).toSQLString(mergedMetadata.getSink().getConnection(), false)
          val firstStepFields = firstStepTableInfo.maybeSchema
            .map { schema =>
              schema.getFields.asScala.map(_.getName)
            }
            .getOrElse(
              throw new Exception(
                "Should never happen in Ingestion mode. We know the fields we are loading using the yml files"
              )
            )
          val req =
            s"SELECT ${firstStepFields.mkString(",")}, '${applicationId()}' as JOBID FROM $targetTable"
          val taskDesc = AutoTaskDesc(
            s"archive-${applicationId()}",
            Some(req),
            database = archiveDatabaseName,
            archiveDomainName,
            archiveTableName,
            Some(WriteMode.APPEND),
            sink = Some(mergedMetadata.getSink().toAllSinks())
          )

          val autoTask = AutoTask.task(
            taskDesc,
            Map.empty,
            None,
            truncate = false
          )(
            settings,
            storageHandler,
            schemaHandler
          )
          autoTask.run()
        }
        Try(firstStepBigqueryJob.dropTable(firstStepTempTable))
          .flatMap(_ => output)
          .recoverWith { case exception =>
            Utils.logException(logger, exception)
            output
          } // ignore exception but log it
      } else {
        val bigqueryJob = new BigQueryNativeJob(targetConfig, "")
        bigqueryJob.loadPathsToBQ(
          bigqueryJob.getTableInfo(targetTableId, _.bqSchemaFinal(schemaHandler))
        )
      }
    }.flatten match {
      case Failure(exception) =>
        logLoadFailureInAudit(start, exception)
      case res @ Success(result) =>
        result.jobResult.job.flatMap(j => Option(j.getStatus.getExecutionErrors)).foreach {
          errors =>
            errors.forEach(err => logger.error(f"${err.getReason} - ${err.getMessage}"))
        }
        logLoadInAudit(
          start,
          result.totalRows,
          result.totalAcceptedRows,
          result.totalRejectedRows
        ) match {
          case Failure(exception) => throw exception
          case Success(auditLog) =>
            if (auditLog.success) res
            else throw new DisallowRejectRecordException()
        }
    }
  }

  private def getArchiveTableComponents(): (Option[String], String, String) = {
    val fullArchiveTableName = Utils.parseJinja(
      settings.appConfig.archiveTablePattern,
      Map("domain" -> domain.finalName, "table" -> schema.finalName)
    )
    val archiveTableComponents = fullArchiveTableName.split('.')
    val (archiveDatabaseName, archiveDomainName, archiveTableName) =
      if (archiveTableComponents.length == 3) {
        (
          Some(archiveTableComponents(0)),
          archiveTableComponents(1),
          archiveTableComponents(2)
        )
      } else if (archiveTableComponents.length == 2) {
        (
          schemaHandler.getDatabase(domain),
          archiveTableComponents(0),
          archiveTableComponents(1)
        )
      } else {
        throw new Exception(
          s"Archive table name must be in the format <domain>.<table> but got $fullArchiveTableName"
        )
      }
    (archiveDatabaseName, archiveDomainName, archiveTableName)
  }

  private def buildCommonNativeBQLoadConfig(
    createDisposition: String,
    writeDisposition: String,
    bqSink: BigQuerySink,
    schemaWithMergedMetadata: Schema
  ): BigQueryLoadConfig = {
    BigQueryLoadConfig(
      connectionRef = Some(mergedMetadata.getConnectionRef()),
      source = Left(path.map(_.toString).mkString(",")),
      outputTableId = None,
      sourceFormat = settings.appConfig.defaultWriteFormat,
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      outputPartition = None,
      outputClustering = Nil,
      days = None,
      requirePartitionFilter = false,
      rls = Nil,
      partitionsToUpdate = Nil,
      starlakeSchema = Some(schemaWithMergedMetadata),
      domainTags = domain.tags,
      domainDescription = domain.comment,
      outputDatabase = schemaHandler.getDatabase(domain),
      dynamicPartitionOverwrite = bqSink.dynamicPartitionOverwrite
    )
  }

  private def computeEffectiveInputSchema(): Schema = {
    mergedMetadata.getFormat() match {
      case Format.DSV =>
        (mergedMetadata.isWithHeader(), path.map(_.toString).headOption) match {
          case (java.lang.Boolean.TRUE, Some(sourceFile)) =>
            val csvHeaders = storageHandler.readAndExecute(
              new Path(sourceFile),
              Charset.forName(mergedMetadata.getEncoding())
            ) { is =>
              Using.resource(is) { reader =>
                assert(mergedMetadata.getQuote().length <= 1, "quote must be a single character")
                assert(mergedMetadata.getEscape().length <= 1, "quote must be a single character")
                val csvParserSettings = new CsvParserSettings()
                val format = new CsvFormat()
                format.setDelimiter(mergedMetadata.getSeparator())
                mergedMetadata.getQuote().headOption.foreach(format.setQuote)
                mergedMetadata.getEscape().headOption.foreach(format.setQuoteEscape)
                csvParserSettings.setFormat(format)
                // allocate twice the declared columns. If fail a strange exception is thrown: https://github.com/uniVocity/univocity-parsers/issues/247
                csvParserSettings.setMaxColumns(schema.attributes.length * 2)
                csvParserSettings.setNullValue(mergedMetadata.getNullValue())
                csvParserSettings.setHeaderExtractionEnabled(true)
                csvParserSettings.setMaxCharsPerColumn(-1)
                val csvParser = new CsvParser(csvParserSettings)
                csvParser.beginParsing(reader)
                // call this in order to get the headers even if there is no record
                csvParser.parseNextRecord()
                csvParser.getRecordMetadata.headers().toList
              }
            }
            val attributesMap = schema.attributes.map(attr => attr.name -> attr).toMap
            val csvAttributesInOrders =
              csvHeaders.map(h =>
                attributesMap.getOrElse(h, Attribute(h, ignore = Some(true), required = false))
              )
            // attributes not in csv input file must not be required but we don't force them to optional.
            val effectiveAttributes =
              csvAttributesInOrders ++ schema.attributes.diff(csvAttributesInOrders)
            schema.copy(attributes = effectiveAttributes)
          case _ => schema
        }
      case _ => schema
    }
  }

  def applyBigQuerySecondStepSQL(
    bigqueryJob: BigQueryNativeJob,
    firstStepTempTableId: TableId,
    targetTableId: TableId,
    targetTableSchema: BQSchema,
    starlakeSchema: Schema
  ): Try[(BigQueryJobResult, Long)] = {
    val sourceUris = path.map(_.toString).mkString(",").replace("'", "\\'")

    // Even if merge is able to handle data deletion, in order to have same behavior with spark
    // we require user to set dynamic partition overwrite
    // we have sql as optional because in dynamic partition overwrite mode, if no partition exists, we do nothing
    val mergeInstructions = starlakeSchema.merge match {
      case Some(mergeOptions: MergeOptions) =>
        handleBQNativeMergeCases(
          bigqueryJob,
          firstStepTempTableId,
          targetTableId,
          starlakeSchema,
          mergeOptions,
          sourceUris
        )
      case None =>
        handleBQNativeNoMergeCases(
          bigqueryJob,
          firstStepTempTableId,
          targetTableId,
          starlakeSchema,
          sourceUris
        )
    }
    mergeInstructions.flatMap { case (sqlOpt, asTable, nullCountValues) =>
      sqlOpt match {
        case Some(sql) =>
          logger.info(s"buildSqlSelect: $sql")
          if (asTable)
            bigqueryJob.runAndSinkAsTable(sqlOpt, Some(targetTableSchema)).map(_ -> nullCountValues)
          else
            bigqueryJob.runInteractiveQuery(sqlOpt).map(_ -> nullCountValues)
        case None =>
          logger.info("Sink skipped")
          Success(BigQueryJobResult(None, 0, None) -> nullCountValues)
      }
    }
  }

  private def handleBQNativeNoMergeCases(
    bigqueryJob: BigQueryNativeJob,
    firstStepTempTableId: TableId,
    targetTableId: TableId,
    schema: Schema,
    sourceUris: String
  ): Try[(Option[String], Boolean, Long)] = {

    val tempTable = getBQFullTableName(firstStepTempTableId)
    val targetTable = getBQFullTableName(targetTableId)

    (
      bigqueryJob.cliConfig.dynamicPartitionOverwrite.getOrElse(true),
      bigqueryJob.cliConfig.outputPartition
    ) match {
      case (true, Some(partitionName)) =>
        val sql = schema.buildSqlSelectOnLoad(tempTable, Some(sourceUris))
        computePartitions(bigqueryJob, partitionName, sql) match {
          case (_, nullCountValues) if nullCountValues > 0 && settings.appConfig.rejectAllOnError =>
            logger.error("Null value found in partition")
            Failure(new NullValueFoundException(nullCountValues))
          case (Nil, nullCountValues) =>
            logger.info(
              "No partitions found in source. In dynamic partition overwrite mode, skip sink."
            )
            Success(None, false, nullCountValues)
          case (allPartitions, nullCountValues) =>
            val inPartitions = allPartitions.map(date =>
              f"'$date'"
            ) mkString (f"date(`$partitionName`) IN (", ",", ")")
            Success(
              Some(
                schema.buildBQSqlMergeOnLoad(
                  tempTable,
                  targetTable,
                  Nil,
                  List(inPartitions),
                  partitionOverwrite = true,
                  Some(sourceUris),
                  targetTableExists = true,
                  mergedMetadata.getSink().getConnection().getJdbcEngineName()
                )
              ),
              false,
              nullCountValues
            )
        }
      case _ => Success(Some(schema.buildSqlSelectOnLoad(tempTable, Some(sourceUris))), true, 0)
    }
  }

  private def getBQFullTableName(targetTableId: TableId) = {
    if (targetTableId.getProject == null)
      s"`${targetTableId.getDataset}.${targetTableId.getTable}`"
    else
      s"`${targetTableId.getProject}.${targetTableId.getDataset}.${targetTableId.getTable}`"
  }

  /** return all partitions and the number of null records */
  private def computePartitions(
    bigqueryJob: BigQueryNativeJob,
    partitionName: String,
    sql: String
  ): (List[String], Long) = {
    val totalColumnName = "total"
    val detectImpliedPartitions =
      s"SELECT cast(date(`$partitionName`) as STRING) AS $partitionName, countif($partitionName IS NULL) AS $totalColumnName FROM ($sql) GROUP BY $partitionName"
    bigqueryJob.runInteractiveQuery(Some(detectImpliedPartitions), pageSize = Some(1000)) match {
      case Failure(exception) => throw exception
      case Success(result) =>
        val (partitions, nullCountValues) = result.tableResult
          .map(
            _.iterateAll()
              .iterator()
              .asScala
              .foldLeft(List[String]() -> 0L) { case ((partitions, nullCount), row) =>
                val updatedPartitions = scala
                  .Option(row.get(partitionName))
                  .filterNot(_.isNull)
                  .map(_.getStringValue) match {
                  case Some(value) => value +: partitions
                  case None        => partitions
                }
                updatedPartitions -> (nullCount + row.get(totalColumnName).getLongValue)
              }
          )
          .getOrElse(Nil -> 0L)
        partitions.sorted -> nullCountValues
    }
  }

  private def handleBQNativeMergeCases(
    bigqueryJob: BigQueryNativeJob,
    tempTableId: TableId,
    targetTableId: TableId,
    starlakeSchema: Schema,
    mergeOptions: MergeOptions,
    sourceUris: String
  ): Try[(Option[String], Boolean, Long)] = {
    val tempTable = getBQFullTableName(tempTableId)
    val targetTable = getBQFullTableName(targetTableId)
    val targetFilters =
      (mergeOptions.queryFilter, bigqueryJob.cliConfig.outputPartition) match {
        case (Some(_), Some(_)) =>
          val existingPartitions = {
            bigqueryJob.bigquery().listPartitions(targetTableId).asScala.toList
          }
          mergeOptions
            .buidlBQQuery(existingPartitions, schemaHandler.activeEnvVars(), options)
            .toList
        case _ => Nil
      }
    (
      bigqueryJob.cliConfig.dynamicPartitionOverwrite.getOrElse(true),
      bigqueryJob.cliConfig.writeDisposition,
      bigqueryJob.cliConfig.outputPartition
    ) match {
      case (true, "WRITE_TRUNCATE", Some(partitionName)) =>
        val sql = starlakeSchema.buildBQSqlMergeOnLoad(
          tempTable,
          targetTable,
          targetFilters,
          Nil,
          partitionOverwrite = false,
          Some(sourceUris),
          targetTableExists = true,
          mergedMetadata.getSink().getConnection().getJdbcEngineName()
        )
        computePartitions(bigqueryJob, partitionName, sql) match {
          case (_, nullCountValues) if nullCountValues > 0 && settings.appConfig.rejectAllOnError =>
            logger.error("Null value found in partition")
            Failure(new NullValueFoundException(nullCountValues))
          case (Nil, nullCountValues) =>
            logger.info(
              "No partitions found in source. In dynamic partition overwrite mode, skip sink."
            )
            Success(None, false, nullCountValues)
          case (allPartitions, nullCountValues) =>
            val inPartitions = allPartitions.map(date =>
              f"'$date'"
            ) mkString (f"date(`$partitionName`) IN (", ",", ")")
            Success(
              Some(
                starlakeSchema.buildBQSqlMergeOnLoad(
                  tempTable,
                  targetTable,
                  targetFilters,
                  List(inPartitions),
                  partitionOverwrite = true,
                  Some(sourceUris),
                  targetTableExists = true,
                  mergedMetadata.getSink().getConnection().getJdbcEngineName()
                )
              ),
              false,
              nullCountValues
            )
        }
      case _ =>
        Success(
          Some(
            starlakeSchema.buildBQSqlMergeOnLoad(
              tempTable,
              targetTable,
              targetFilters,
              Nil,
              partitionOverwrite = false,
              Some(sourceUris),
              targetTableExists = true,
              mergedMetadata.getSink().getConnection().getJdbcEngineName()
            )
          ),
          true,
          0
        )
    }
  }

  private def applyBigQuerySecondStep(
    targetBigqueryJob: BigQueryNativeJob,
    firstStepTempTableId: TableId,
    targetTableSchema: BQSchema,
    starlakeSchema: Schema
  ): Try[(BigQueryJobResult, Long)] = {
    targetBigqueryJob.cliConfig.outputTableId
      .map { targetTableId =>
        updateTargetTableSchema(targetBigqueryJob, targetTableSchema)
        applyBigQuerySecondStepSQL(
          targetBigqueryJob,
          firstStepTempTableId,
          targetTableId,
          targetTableSchema,
          starlakeSchema
        )
      }
      .getOrElse(throw new Exception("Should never happen"))
  }

  private def updateTargetTableSchema(
    bigqueryJob: BigQueryNativeJob,
    incomingTableSchema: BQSchema
  ): Try[StandardTableDefinition] = {
    val tableId = bigqueryJob.tableId
    if (bigqueryJob.tableExists(tableId)) {
      val existingTableSchema = bigqueryJob.getBQSchema(tableId)
      // detect new columns
      val newColumns = incomingTableSchema.getFields.asScala
        .filterNot(field =>
          existingTableSchema.getFields.asScala.exists(_.getName == field.getName)
        )
        .toList
      // Update target table schema with new columns if any
      if (newColumns.nonEmpty) {
        bigqueryJob.updateTableSchema(tableId, incomingTableSchema)
      } else
        Try(bigqueryJob.getTableDefinition(tableId))
    } else {
      val config = bigqueryJob.cliConfig
      val partitionField = config.outputPartition.map { partitionField =>
        FieldPartitionInfo(partitionField, config.days, config.requirePartitionFilter)
      }
      val clusteringFields = config.outputClustering match {
        case Nil    => None
        case fields => Some(ClusteringInfo(fields.toList))
      }
      bigqueryJob.getOrCreateTable(
        config.domainDescription,
        TableInfo(
          tableId,
          schema.comment,
          Some(incomingTableSchema),
          partitionField,
          clusteringFields
        ),
        None
      ) map { case (table, definition) => definition }
    }
  }

  private def logLoadFailureInAudit(start: Timestamp, exception: Throwable): Failure[Nothing] = {
    val end = Timestamp.from(Instant.now())
    val err = Utils.exceptionAsString(exception)
    val log = AuditLog(
      applicationId(),
      Some(path.map(_.toString).mkString(",")),
      domain.name,
      schema.name,
      success = false,
      0,
      0,
      0,
      start,
      end.getTime - start.getTime,
      err,
      Step.LOAD.toString,
      schemaHandler.getDatabase(domain),
      settings.appConfig.tenant
    )
    AuditLog.sink(log)(settings, storageHandler, schemaHandler)
    logger.error(err)
    Failure(exception)
  }

  private def logLoadInAudit(
    start: Timestamp,
    inputCount: Long,
    acceptedCount: Long,
    rejectedCount: Long
  ): Try[AuditLog] = {
    val inputFiles = path.map(_.toString).mkString(",")
    logger.info(
      s"ingestion-summary -> files: [$inputFiles], domain: ${domain.name}, schema: ${schema.name}, input: $inputCount, accepted: $acceptedCount, rejected:$rejectedCount"
    )
    val end = Timestamp.from(Instant.now())
    val success = !settings.appConfig.rejectAllOnError || rejectedCount == 0
    val log = AuditLog(
      applicationId(),
      Some(inputFiles),
      domain.name,
      schema.name,
      success = success,
      inputCount,
      acceptedCount,
      rejectedCount,
      start,
      end.getTime - start.getTime,
      if (success) "success" else s"$rejectedCount invalid records",
      Step.LOAD.toString,
      schemaHandler.getDatabase(domain),
      settings.appConfig.tenant
    )
    AuditLog.sink(log)(settings, storageHandler, schemaHandler).map(_ => log)
  }

  ///////////////////////////////////////////////////////////////////////////
  /////// SPARK ENGINE ONLY /////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////
  /** Main entry point as required by the Spark Job interface
    *
    * @return
    *   : Spark Session used for the job
    */
  def runSpark(): Try[JobResult] = {
    session.sparkContext.setLocalProperty(
      "spark.scheduler.pool",
      settings.appConfig.sparkScheduling.poolName
    )
    val jobResult = domain.checkValidity(schemaHandler) match {
      case Left(errors) =>
        val errs = errors.map(_.toString()).reduce { (errs, err) =>
          errs + "\n" + err
        }
        Failure(throw new Exception(s"-- $name --\n" + errs))
      case Right(_) =>
        val start = Timestamp.from(Instant.now())
        runPreSql()
        val dataset = loadDataSet(false)
        dataset match {
          case Success(dataset) =>
            Try {
              val (rejectedDS, acceptedDS, rejectedCount) = ingest(dataset)
              val inputCount = dataset.count()
              val totalAcceptedCount = acceptedDS.count() - rejectedCount
              val totalRejectedCount = rejectedDS.count() + rejectedCount
              (inputCount, totalAcceptedCount, totalRejectedCount)
            }.recoverWith { case exception =>
              logLoadFailureInAudit(start, exception)
            }.map { case (inputCount, acceptedCount, rejectedCount) =>
              logLoadInAudit(start, inputCount, acceptedCount, rejectedCount) match {
                case Failure(exception) => throw exception
                case Success(auditLog) =>
                  if (auditLog.success) SparkJobResult(None)
                  else throw new DisallowRejectRecordException()
              }
            }
          case Failure(exception) =>
            logLoadFailureInAudit(start, exception)
        }
    }
    // After each ingestionjob we explicitely clear the spark cache
    session.catalog.clearCache()
    jobResult
  }

  // /////////////////////////////////////////////////////////////////////////
  // region Merge between the target and the source Dataframe
  // /////////////////////////////////////////////////////////////////////////

  private def mergeFromParquet(
    acceptedPath: Path,
    withScriptFieldsDF: DataFrame,
    mergeOptions: MergeOptions
  ): (DataFrame, List[String]) = {
    val incomingSchema = schema.finalSparkSchema(schemaHandler)

    val fileFound = Try {
      storageHandler.list(acceptedPath, recursive = true).nonEmpty ||
      storageHandler.listDirectories(acceptedPath).nonEmpty
    }.getOrElse(false)

    val existingDF =
      if (fileFound) {
        // Load from accepted area
        // We provide the accepted DF schema since partition columns types are inferred when parquet is loaded and might not match with the DF being ingested
        Try {
          val compatibleSchema = MergeUtils.computeCompatibleSchema(
            session.read
              .format(settings.appConfig.defaultWriteFormat)
              .load(acceptedPath.toString)
              .schema,
            incomingSchema
          )

          session.read
            .schema(
              compatibleSchema
            )
            .format(settings.appConfig.defaultWriteFormat)
            .load(acceptedPath.toString)
        } getOrElse {
          logger.warn(s"Empty folder $acceptedPath")
          session.createDataFrame(session.sparkContext.emptyRDD[Row], withScriptFieldsDF.schema)
        }
      } else
        session.createDataFrame(session.sparkContext.emptyRDD[Row], withScriptFieldsDF.schema)

    val partitionedInputDF =
      partitionDataset(withScriptFieldsDF, mergedMetadata.getPartitionAttributes())
    logger.whenInfoEnabled {
      logger.info(s"partitionedInputDF field count=${partitionedInputDF.schema.fields.length}")
      logger.info(
        s"partitionedInputDF field list=${partitionedInputDF.schema.fieldNames.mkString(",")}"
      )
    }
    val (finalIncomingDF, mergedDF, _) = {
      MergeUtils.computeToMergeAndToDeleteDF(existingDF, partitionedInputDF, mergeOptions)
    }
    (mergedDF, Nil)
  }

  /** In the queryFilter, the user may now write something like this : `partitionField in last(3)`
    * this will be translated to partitionField between partitionStart and partitionEnd
    *
    * partitionEnd is the last partition in the dataset paritionStart is the 3rd last partition in
    * the dataset
    *
    * if partititionStart or partitionEnd does nos exist (aka empty dataset) they are replaced by
    * 19700101
    *
    * @param incomingDF
    * @param mergeOptions
    * @return
    */
  private def mergeFromBQ(
    incomingDF: DataFrame,
    mergeOptions: MergeOptions,
    sink: BigQuerySink
  ): (DataFrame, List[String]) = {
    // When merging to BigQuery, load existing DF from BigQuery
    val tableMetadata =
      BigQuerySparkJob.getTable(domain.finalName + "." + schema.finalName)
    val existingDF = tableMetadata.table
      .map { table =>
        val incomingSchema = BigQueryUtils.normalizeSchema(schema.finalSparkSchema(schemaHandler))
        val updatedTable = updateBqTableSchema(table, incomingSchema)
        val bqTable = s"${domain.finalName}.${schema.finalName}"
        // We provided the acceptedDF schema here since BQ lose the required / nullable information of the schema
        val existingBQDFWithoutFilter = session.read
          .schema(incomingSchema)
          .format("bigquery")
          .option("table", bqTable)

        val existingBigQueryDFReader = (mergeOptions.queryFilter, sink.timestamp) match {
          case (Some(_), Some(_)) =>
            val partitions =
              tableMetadata.biqueryClient.listPartitions(updatedTable.getTableId).asScala.toList
            val filter =
              mergeOptions.buidlBQQuery(partitions, schemaHandler.activeEnvVars(), options)
            existingBQDFWithoutFilter
              .option("filter", filter.getOrElse(throw new Exception("should never happen")))
          case (_, _) =>
            existingBQDFWithoutFilter
        }
        existingBigQueryDFReader.load()
      }
      .getOrElse {
        session.createDataFrame(session.sparkContext.emptyRDD[Row], incomingDF.schema)
      }

    val (finalIncomingDF, mergedDF, toDeleteDF) =
      MergeUtils.computeToMergeAndToDeleteDF(existingDF, incomingDF, mergeOptions)

    val partitionOverwriteMode = {
      sink.dynamicPartitionOverwrite
        .map {
          case true  => "STATIC"
          case false => "DYNAMIC"
        }
        .getOrElse(
          session.conf.get("spark.sql.sources.partitionOverwriteMode", "STATIC").toUpperCase()
        )
    }
    val partitionsToUpdate = (
      partitionOverwriteMode,
      sink.timestamp,
      settings.appConfig.mergeOptimizePartitionWrite
    ) match {
      // no need to apply optimization if existing dataset is empty
      case ("DYNAMIC", Some(timestamp), true) if !existingDF.isEmpty =>
        logger.info(s"Computing partitions to update on date column $timestamp")
        val partitionsToUpdate =
          BigQueryUtils.computePartitionsToUpdateAfterMerge(finalIncomingDF, toDeleteDF, timestamp)
        logger.info(
          s"The following partitions will be updated ${partitionsToUpdate.mkString(",")}"
        )
        partitionsToUpdate
      case ("STATIC", _, _) | ("DYNAMIC", _, _) =>
        Nil
      case (_, _, _) =>
        throw new Exception("Should never happen")
    }

    (mergedDF, partitionsToUpdate)
  }

  // /////////////////////////////////////////////////////////////////////////
  // endregion
  // /////////////////////////////////////////////////////////////////////////

  def reorderAttributes(dataFrame: DataFrame): List[Attribute] = {
    val finalSchema = schema.attributesWithoutScriptedFields :+ Attribute(
      name = CometColumns.cometInputFileNameColumn
    )
    val attributesMap =
      finalSchema.map(attr => (attr.name, attr)).toMap
    dataFrame.columns.map(colName => attributesMap(colName)).toList
  }

  private def updateBqTableSchema(table: Table, incomingSchema: StructType): Table = {
    // This will raise an exception if schemas are not compatible.
    val existingSchema = BigQuerySchemaConverters.toSpark(
      table.getDefinition[StandardTableDefinition].getSchema
    )

    MergeUtils.computeCompatibleSchema(existingSchema, incomingSchema)
    val newBqSchema =
      BigQueryUtils.bqSchema(
        BigQueryUtils.normalizeCompatibleSchema(
          schema.finalSparkSchema(schemaHandler),
          existingSchema
        )
      )
    val updatedTableDefinition =
      table.getDefinition[StandardTableDefinition].toBuilder.setSchema(newBqSchema).build()
    val updatedTable =
      table.toBuilder.setDefinition(updatedTableDefinition).build()
    updatedTable.update()
  }

  /** Save typed dataset in parquet. If hive support is active, also register it as a Hive Table and
    * if analyze is active, also compute basic statistics
    *
    * @param dataset
    *   : dataset to save
    * @param targetPath
    *   : absolute path
    * @param writeMode
    *   : Append or overwrite
    * @param area
    *   : accepted or rejected area
    */
  private def sinkToFile(
    dataset: DataFrame,
    targetPath: Path,
    writeMode: WriteMode,
    area: StorageArea,
    merge: Boolean,
    writeFormat: String
  ): DataFrame = {
    val resultDataFrame = if (dataset.columns.length > 0) {
      val saveMode = writeMode.toSaveMode
      val hiveDB = StorageArea.area(domain.finalName, Some(area))
      val tableName = schema.name
      val fullTableName = s"$hiveDB.$tableName"
      if (settings.appConfig.isHiveCompatible()) {
        val dbComment = domain.comment.getOrElse("")
        val tableTagPairs = Utils.extractTags(domain.tags) + ("comment" -> dbComment)
        val tagsAsString = tableTagPairs.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
        session.sql(s"CREATE DATABASE IF NOT EXISTS $hiveDB WITH DBPROPERTIES($tagsAsString)")
        session.sql(s"use $hiveDB")
        Try {
          if (writeMode.toSaveMode == SaveMode.Overwrite)
            session.sql(s"DROP TABLE IF EXISTS $hiveDB.$tableName")
        } match {
          case Success(_) => ;
          case Failure(e) =>
            logger.warn("Ignore error when hdfs files not found")
            Utils.logException(logger, e)
        }
      }

      val sinkOpt = mergedMetadata.getSink()
      val (sinkPartition, nbPartitions, clustering, options, dynamicPartitionOption) =
        sinkOpt match {
          case fsSink: FsSink =>
            val partitionColumns = fsSink.partition

            val sinkClustering = fsSink.clustering.orElse(None)
            val sinkOptions = fsSink.options.orElse(None)
            val dynamicPartitionOverwrite = fsSink.dynamicPartitionOverwrite
              .map {
                case true  => Map("partitionOverwriteMode" -> "DYNAMIC")
                case false => Map("partitionOverwriteMode" -> "STATIC")
              }
              .getOrElse(Map.empty)
            (
              partitionColumns,
              nbFsPartitions(dataset),
              sinkClustering,
              sinkOptions.getOrElse(Map.empty),
              dynamicPartitionOverwrite
            )
          case _ =>
            (
              None,
              nbFsPartitions(dataset),
              mergedMetadata.getClustering(),
              Map.empty[String, String],
              Map.empty[String, String]
            )
        }

      // No need to apply partition on rejected dF
      val partitionedDFWriter = {
        val writer =
          if (area == StorageArea.rejected)
            partitionedDatasetWriter(dataset.repartition(nbPartitions), Nil)
          else
            partitionedDatasetWriter(
              dataset.repartition(nbPartitions),
              sinkPartition.map(_.getAttributes()).getOrElse(Nil)
            )
        writer.options(dynamicPartitionOption)
      }

      val clusteredDFWriter = clustering match {
        case None          => partitionedDFWriter
        case Some(columns) => partitionedDFWriter.sortBy(columns.head, columns.tail: _*)
      }

      val mergePath = s"${targetPath.toString}.merge"
      val (targetDatasetWriter, finalDataset) = if (merge && area != StorageArea.rejected) {
        logger.info(s"Saving Dataset to merge location $mergePath")
        clusteredDFWriter
          .mode(SaveMode.Overwrite)
          .format(writeFormat)
          .options(options ++ dynamicPartitionOption)
          .option("path", mergePath)
          .save()
        logger.info(s"reading Dataset from merge location $mergePath")
        val mergedDataset =
          session.read.format(settings.appConfig.defaultWriteFormat).load(mergePath)
        (
          partitionedDatasetWriter(
            mergedDataset,
            sinkPartition.map(_.getAttributes()).getOrElse(Nil)
          ),
          mergedDataset
        )
      } else
        (clusteredDFWriter, dataset)
      // We do not output empty datasets
      if (!finalDataset.isEmpty) {
        val finalTargetDatasetWriter =
          if (csvOutput() && area != StorageArea.rejected) {
            targetDatasetWriter
              .mode(saveMode)
              .format("csv")
              .option("ignoreLeadingWhiteSpace", value = false)
              .option("ignoreTrailingWhiteSpace", value = false)
              .option("header", false) // header generated manually if any
              .option("delimiter", mergedMetadata.separator.getOrElse("µ"))
              .option("path", targetPath.toString)
          } else
            targetDatasetWriter
              .mode(saveMode)
              .format(writeFormat)
              .option("path", targetPath.toString)

        logger.info(s"Saving Dataset to final location $targetPath in $saveMode mode")

        if (settings.appConfig.isHiveCompatible()) {
          finalTargetDatasetWriter.saveAsTable(fullTableName)
          val tableComment = schema.comment.getOrElse("")
          val tableTagPairs = Utils.extractTags(schema.tags) + ("comment" -> tableComment)
          val tagsAsString = tableTagPairs.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
          session.sql(
            s"ALTER TABLE $fullTableName SET TBLPROPERTIES($tagsAsString)"
          )
          analyze(fullTableName)
        } else {
          finalTargetDatasetWriter.save()
        }
        if (merge && area != StorageArea.rejected) {
          // Here we read the df from the targetPath and not the merged one since that on is gonna be removed
          // However, we keep the merged DF schema so we don't lose any metadata from reloading the final parquet (especially the nullables)
          val df = session.createDataFrame(
            session.read
              .format(settings.appConfig.defaultWriteFormat)
              .load(targetPath.toString)
              .rdd,
            finalDataset.schema
          )
          storageHandler.delete(new Path(mergePath))
          logger.info(s"deleted merge file $mergePath")
          df
        } else
          finalDataset
      } else {
        finalDataset
      }
    } else {
      logger.warn("Empty dataset with no columns won't be saved")
      session.emptyDataFrame
    }
    if (csvOutput() && area != StorageArea.rejected) {
      val outputList = storageHandler
        .list(targetPath, ".csv", LocalDateTime.MIN, recursive = false)
        .map(_.path)
        .filterNot(path => schema.pattern.matcher(path.getName).matches())
      if (outputList.nonEmpty) {
        val finalCsvPath =
          if (csvOutputExtension().nonEmpty) {
            // Explicitly set extension
            val targetName = path.head.getName
            val index = targetName.lastIndexOf('.')
            val filePrefix = if (index > 0) targetName.substring(0, index) else targetName
            val finalName = filePrefix + csvOutputExtension()
            new Path(targetPath, finalName)
          } else {
            new Path(targetPath, path.head.getName)
          }
        val withHeader = mergedMetadata.isWithHeader()
        val delimiter = mergedMetadata.separator.getOrElse("µ")
        val header =
          if (withHeader)
            Some(this.schema.attributes.map(_.getFinalName()).mkString(delimiter))
          else None
        storageHandler.copyMerge(header, targetPath, finalCsvPath, deleteSource = true)
        // storageHandler.move(csvPath, finalCsvPath)
      }
    }
    // output file should have the same name as input file when applying privacy
    if (
      settings.appConfig.defaultWriteFormat == "text" && settings.appConfig.privacyOnly && area != StorageArea.rejected
    ) {
      val pathsOutput = storageHandler
        .list(targetPath, ".txt", LocalDateTime.MIN, recursive = false)
        .map(_.path)
        .filterNot(path => schema.pattern.matcher(path.getName).matches())
      if (pathsOutput.nonEmpty) {
        val txtPath = pathsOutput.head
        val finalTxtPath = new Path(
          targetPath,
          path.head.getName
        )
        storageHandler.move(txtPath, finalTxtPath)
      }
    }
    applyHiveTableAcl()
    resultDataFrame
  }

  private def nbFsPartitions(
    dataset: DataFrame
  ): Int = {
    if (dataset.rdd.getNumPartitions == 0) // avoid error for an empty dataset
      1
    else
      dataset.rdd.getNumPartitions
  }

  private def runExpectations(acceptedDF: DataFrame): Try[JobResult] = {
    if (settings.appConfig.expectations.active) {
      new ExpectationJob(
        schemaHandler.getDatabase(this.domain),
        this.domain.finalName,
        this.schema.finalName,
        this.schema.expectations,
        storageHandler,
        schemaHandler,
        Some(Left(acceptedDF)),
        new SparkExpectationAssertionHandler(session)
      ).run()
    } else {
      Success(SparkJobResult(None))
    }
  }

  private def runExpectations(
    domainAndTableName: String,
    connection: java.sql.Connection
  ): Try[JobResult] = {
    if (settings.appConfig.expectations.active) {

      new ExpectationJob(
        schemaHandler.getDatabase(this.domain),
        this.domain.finalName,
        this.schema.finalName,
        this.schema.expectations,
        storageHandler,
        schemaHandler,
        Some(Right(domainAndTableName)),
        new JdbcExpectationAssertionHandler(connection)
      ).run()
    } else {
      Success(SparkJobResult(None))
    }
  }

  private def runExpectations(
    domainAndTableName: String,
    job: BigQueryNativeJob
  ): Try[JobResult] = {
    if (settings.appConfig.expectations.active) {
      new ExpectationJob(
        schemaHandler.getDatabase(this.domain),
        this.domain.finalName,
        this.schema.finalName,
        this.schema.expectations,
        storageHandler,
        schemaHandler,
        Some(Right(domainAndTableName)),
        new BigQueryExpectationAssertionHandler(job)
      ).run()
    } else {
      Success(SparkJobResult(None))
    }
  }

  private def runMetrics(acceptedDF: DataFrame) = {
    if (settings.appConfig.metrics.active) {
      new MetricsJob(
        this.domain,
        this.schema,
        this.storageHandler,
        this.schemaHandler
      )
        .run(acceptedDF, System.currentTimeMillis())
    }
  }

  private def dfWithAttributesRenamed(acceptedDF: DataFrame): DataFrame = {
    val renamedAttributes = schema.renamedAttributes().toMap
    logger.whenInfoEnabled {
      renamedAttributes.foreach { case (name, rename) =>
        logger.info(s"renaming column $name to $rename")
      }
    }
    val finalDF =
      renamedAttributes.foldLeft(acceptedDF) { case (acc, (name, rename)) =>
        acc.withColumnRenamed(existingName = name, newName = rename)
      }
    finalDF
  }

  /** Merge new and existing dataset if required Save using overwrite / Append mode
    *
    * @param validationResult
    */
  protected def saveAccepted(
    validationResult: ValidationResult
  ): Try[(DataFrame, Path, Long)] = {
    if (!settings.appConfig.rejectAllOnError || validationResult.rejected.isEmpty) {
      val acceptedPath =
        new Path(DatasetArea.accepted(domain.finalName), schema.finalName)
      logger.whenDebugEnabled {
        logger.debug(s"acceptedRDD SIZE ${validationResult.accepted.count()}")
        logger.debug(validationResult.accepted.showString(1000))
      }

      val finalAcceptedDF = computeFinalDF(validationResult.accepted)
      val runExpectationsResult = runExpectations(finalAcceptedDF)
      runExpectationsResult
        .flatMap { _ =>
          val (mergedDF, partitionsToUpdate) = applyMerge(acceptedPath, finalAcceptedDF)
          val finalMergedDf: DataFrame = runPostSQL(mergedDF)
          logger.whenInfoEnabled {
            logger.info("Final Dataframe Schema")
            logger.info(finalMergedDf.schemaString())
          }
          sinkAccepted(finalMergedDf, partitionsToUpdate)
        }
        .map { case (sinkedDF, rejectedRecordCount) =>
          runMetrics(finalAcceptedDF)
          (sinkedDF, acceptedPath, rejectedRecordCount)
        }
    } else {
      Success(session.emptyDataFrame, new Path("invalid-path"), 0)
    }
  }
  private def computeFinalDF(accepted: DataFrame): DataFrame = {
    val acceptedRenamedFields = dfWithAttributesRenamed(accepted)

    val acceptedDfWithScriptFields: DataFrame = computeScriptedAttributes(
      acceptedRenamedFields
    )

    val acceptedDfWithScriptAndTransformedFields: DataFrame = computeTransformedAttributes(
      acceptedDfWithScriptFields
    )

    val acceptedDfFiltered = filterData(acceptedDfWithScriptAndTransformedFields)

    val acceptedDfWithoutIgnoredFields: DataFrame = removeIgnoredAttributes(
      acceptedDfFiltered
    )

    val acceptedDF = acceptedDfWithoutIgnoredFields.drop(CometColumns.cometInputFileNameColumn)
    val finalAcceptedDF: DataFrame =
      computeFinalSchema(acceptedDF).persist(settings.appConfig.cacheStorageLevel)
    finalAcceptedDF
  }

  private def filterData(acceptedDfWithScriptAndTransformedFields: DataFrame): Dataset[Row] = {
    schema.filter
      .map { filterExpr =>
        logger.info(s"Applying data filter: $filterExpr")
        acceptedDfWithScriptAndTransformedFields.filter(filterExpr)
      }
      .getOrElse(acceptedDfWithScriptAndTransformedFields)
  }

  private def applyMerge(
    acceptedPath: Path,
    finalAcceptedDF: DataFrame
  ): (DataFrame, List[String]) = {
    val (mergedDF, partitionsToUpdate) =
      schema.merge.fold((finalAcceptedDF, List.empty[String])) { mergeOptions =>
        mergedMetadata.getSink() match {
          case sink: BigQuerySink => mergeFromBQ(finalAcceptedDF, mergeOptions, sink)
          case _: JdbcSink        =>
            // mergeFromJdbc(finalAcceptedDF, mergeOptions)
            throw new Exception(
              "Jdbc merge not supported when spark loader is used. Use native loader instead"
            )

          case _ => mergeFromParquet(acceptedPath, finalAcceptedDF, mergeOptions)
        }
      }

    if (settings.appConfig.mergeForceDistinct) {
      (mergedDF.distinct(), partitionsToUpdate)
    } else {
      (mergedDF, partitionsToUpdate)
    }
  }

  private def computeFinalSchema(acceptedDfWithoutIgnoredFields: DataFrame) = {
    val finalAcceptedDF: DataFrame = if (schema.attributes.exists(_.script.isDefined)) {
      logger.whenDebugEnabled {
        logger.debug("Accepted Dataframe schema right after adding computed columns")
        logger.debug(acceptedDfWithoutIgnoredFields.schemaString())
      }
      // adding computed columns can change the order of columns, we must force the order defined in the schema
      val cols = schema.finalAttributeNames().map(col)
      val orderedWithScriptFieldsDF = acceptedDfWithoutIgnoredFields.select(cols: _*)
      logger.whenDebugEnabled {
        logger.debug("Accepted Dataframe schema after applying the defined schema")
        logger.debug(orderedWithScriptFieldsDF.schemaString())
      }
      orderedWithScriptFieldsDF
    } else {
      acceptedDfWithoutIgnoredFields
    }
    finalAcceptedDF
  }

  private def removeIgnoredAttributes(
    acceptedDfWithScriptAndTransformedFields: DataFrame
  ): DataFrame = {
    val ignoredAttributes = schema.attributes.filter(_.isIgnore()).map(_.getFinalName())
    val acceptedDfWithoutIgnoredFields =
      acceptedDfWithScriptAndTransformedFields.drop(ignoredAttributes: _*)
    acceptedDfWithoutIgnoredFields
  }

  private def computeTransformedAttributes(acceptedDfWithScriptFields: DataFrame): DataFrame = {
    val sqlAttributes = schema.attributes.filter(_.getPrivacy().sql).filter(_.transform.isDefined)
    sqlAttributes.foldLeft(acceptedDfWithScriptFields) { case (df, attr) =>
      df.withColumn(
        attr.getFinalName(),
        expr(
          attr.transform
            .getOrElse(throw new Exception("Should never happen"))
            .richFormat(schemaHandler.activeEnvVars(), options)
        )
          .cast(attr.primitiveSparkType(schemaHandler))
      )
    }
  }

  private def computeScriptedAttributes(acceptedDF: DataFrame): DataFrame = {
    schema.attributes
      .filter(_.script.isDefined)
      .map(attr => (attr.getFinalName(), attr.sparkType(schemaHandler), attr.script))
      .foldLeft(acceptedDF) { case (df, (name, sparkType, script)) =>
        df.withColumn(
          name,
          expr(script.getOrElse("").richFormat(schemaHandler.activeEnvVars(), options))
            .cast(sparkType)
        )
      }
  }

  private def esSink(mergedDF: DataFrame, sink: EsSink): DataFrame = {
    val config = ESLoadConfig(
      timestamp = sink.timestamp,
      id = sink.id,
      format = settings.appConfig.defaultWriteFormat,
      domain = domain.name,
      schema = schema.name,
      dataset = Some(Right(mergedDF)),
      options = sink.connectionRefOptions(settings.appConfig.connectionRef)
    )
    new ESLoadJob(config, storageHandler, schemaHandler).run()
    mergedDF
  }

  private def sinkAccepted(
    mergedDF: DataFrame,
    partitionsToUpdate: List[String]
  ): Try[(DataFrame, Long)] = {
    val result: Try[Try[(DataFrame, Long)]] = Try {
      (mergedMetadata.getSink(), getConnectionType()) match {
        case (sink: EsSink, _) =>
          Success(esSink(mergedDF, sink) -> 0)
        case (sink: BigQuerySink, _) =>
          bqSink(mergedDF, partitionsToUpdate, sink)
        case (_, ConnectionType.KAFKA) =>
          Success(kafkaSink(mergedDF) -> 0)

        case (sink: JdbcSink, _) =>
          Success(genericSink(mergedDF, sink) -> 0)

        case (_, ConnectionType.FS) =>
          val acceptedPath =
            new Path(DatasetArea.accepted(domain.finalName), schema.finalName)
          val sinkedDF = sinkToFile(
            mergedDF,
            acceptedPath,
            getWriteMode(),
            StorageArea.accepted,
            schema.merge.isDefined,
            settings.appConfig.defaultWriteFormat
          )
          Success(sinkedDF -> 0)
        case (_, st: ConnectionType) =>
          logger.trace(s"Unsupported Sink $st")
          Success(mergedDF -> 0)
      }
    }
    result.flatten
  }

  private def kafkaSink(mergedDF: DataFrame): DataFrame = {
    Utils.withResources(new KafkaClient(settings.appConfig.kafka)) { kafkaClient =>
      kafkaClient.sinkToTopic(settings.appConfig.kafka.topics(schema.finalName), mergedDF)
    }
    mergedDF
  }

  private def genericSink(mergedDF: DataFrame, sink: JdbcSink): DataFrame = {
    val (createDisposition: CreateDisposition, writeDisposition: WriteDisposition) = {

      val (cd, wd) = Utils.getDBDisposition(
        mergedMetadata.getWrite(),
        schema.merge.exists(_.key.nonEmpty),
        isJDBC = true
      )
      (CreateDisposition.valueOf(cd), WriteDisposition.valueOf(wd))
    }
    val jdbcConfig = JdbcConnectionLoadCmd.fromComet(
      sink.connectionRef.getOrElse(settings.appConfig.connectionRef),
      settings.appConfig,
      Right(mergedDF),
      outputTable = domain.finalName + "." + schema.finalName,
      createDisposition = createDisposition,
      writeDisposition = writeDisposition
    )

    val res = new sparkJdbcLoader(jdbcConfig).run()
    res match {
      case Success(_) => ;
      case Failure(e) =>
        throw e
    }
    mergedDF
  }

  private def bqSink(
    mergedDF: DataFrame,
    partitionsToUpdate: List[String],
    sink: BigQuerySink
  ): Try[(DataFrame, Long)] = {
    val (createDisposition: String, writeDisposition: String) = Utils.getDBDisposition(
      mergedMetadata.getWrite(),
      schema.merge.exists(_.key.nonEmpty),
      isJDBC = false
    )

    /* We load the schema from the postsql returned dataframe if any */
    val tableSchema = schema.postsql match {
      case Nil => Some(schema.bqSchemaFinal(schemaHandler))
      case _   => Some(BigQueryUtils.bqSchema(mergedDF.schema))
    }
    val config = BigQueryLoadConfig(
      connectionRef = Some(mergedMetadata.getConnectionRef()),
      source = Right(mergedDF),
      outputTableId = Some(
        BigQueryJobBase
          .extractProjectDatasetAndTable(
            schemaHandler.getDatabase(domain),
            domain.finalName,
            schema.finalName
          )
      ),
      sourceFormat = settings.appConfig.defaultWriteFormat,
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      outputPartition = sink.timestamp,
      outputClustering = sink.clustering.getOrElse(Nil),
      days = sink.days,
      requirePartitionFilter = sink.requirePartitionFilter.getOrElse(false),
      rls = schema.rls,
      partitionsToUpdate = partitionsToUpdate,
      starlakeSchema = Some(schema),
      domainTags = domain.tags,
      domainDescription = domain.comment,
      outputDatabase = schemaHandler.getDatabase(domain),
      dynamicPartitionOverwrite = sink.dynamicPartitionOverwrite
    )
    val res = new BigQuerySparkJob(
      config,
      tableSchema,
      schema.comment
    ).run()
    res.map {
      case jobResult: SparkJobResult => mergedDF -> jobResult.rejectedCount
      case el                        => throw new RuntimeException(f"$el should never happen")
    }
  }

  @nowarn
  protected def applyIgnore(dfIn: DataFrame): Dataset[Row] = {
    import session.implicits._
    mergedMetadata.ignore.map { ignore =>
      if (ignore.startsWith("udf:")) {
        dfIn.filter(
          !call_udf(ignore.substring("udf:".length), struct(dfIn.columns.map(dfIn(_)): _*))
        )
      } else {
        dfIn.filter(!($"value" rlike ignore))
      }
    } getOrElse dfIn
  }

  protected def loadDataSet(withSchema: Boolean): Try[DataFrame]

  protected def saveRejected(
    errMessagesDS: Dataset[String],
    rejectedLinesDS: Dataset[String]
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[Path] = {
    logger.whenDebugEnabled {
      logger.debug(s"rejectedRDD SIZE ${errMessagesDS.count()}")
      errMessagesDS.take(100).foreach(rejected => logger.debug(rejected.replaceAll("\n", "|")))
    }
    val domainName = domain.name
    val schemaName = schema.name

    val start = Timestamp.from(Instant.now())
    val formattedDate = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(start)

    if (settings.appConfig.sinkReplayToFile && !rejectedLinesDS.isEmpty) {
      val replayArea = DatasetArea.replay(domainName)
      val targetPath =
        new Path(replayArea, s"$domainName.$schemaName.$formattedDate.replay")
      rejectedLinesDS
        .repartition(1)
        .write
        .format("text")
        .save(targetPath.toString)
      storageHandler.moveSparkPartFile(
        targetPath,
        "0000" // When saving as text file, no extension is added.
      )
    }

    IngestionUtil.sinkRejected(
      session,
      errMessagesDS,
      domainName,
      schemaName,
      now
    ) match {
      case Success((rejectedDF, rejectedPath)) =>
        settings.appConfig.audit.sink.getSink() match {
          case _: FsSink =>
            sinkToFile(
              rejectedDF,
              rejectedPath,
              WriteMode.APPEND,
              StorageArea.rejected,
              merge = false,
              settings.appConfig.defaultRejectedWriteFormat
            )
          case _ => // do nothing
        }
        Success(rejectedPath)
      case Failure(exception) =>
        logger.error("Failed to save Rejected", exception)
        Failure(exception)
    }
  }
}

object IngestionUtil {

  private val rejectedCols = List(
    ("jobid", LegacySQLTypeName.STRING, StringType),
    ("timestamp", LegacySQLTypeName.TIMESTAMP, TimestampType),
    ("domain", LegacySQLTypeName.STRING, StringType),
    ("schema", LegacySQLTypeName.STRING, StringType),
    ("error", LegacySQLTypeName.STRING, StringType),
    ("path", LegacySQLTypeName.STRING, StringType)
  )

  private def bigqueryRejectedSchema(): BQSchema = {
    val fields = rejectedCols map { case (attrName, attrLegacyType, attrStandardType) =>
      Field
        .newBuilder(attrName, attrLegacyType)
        .setMode(Field.Mode.NULLABLE)
        .setDescription("")
        .build()
    }
    BQSchema.of(fields: _*)
  }

  def sinkRejected(
    session: SparkSession,
    rejectedDS: Dataset[String],
    domainName: String,
    schemaName: String,
    now: Timestamp
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[(Dataset[Row], Path)] = {
    import session.implicits._
    val rejectedPath = new Path(DatasetArea.rejected(domainName), schemaName)
    val rejectedPathName = rejectedPath.toString
    // We need to save first the application ID
    // referencing it inside the worker (rdd.map) below would fail.
    val applicationId = session.sparkContext.applicationId
    val rejectedTypedDS = rejectedDS.map { err =>
      RejectedRecord(
        applicationId,
        now,
        domainName,
        schemaName,
        err,
        rejectedPathName
      )
    }
    val rejectedDF = rejectedTypedDS
      .limit(settings.appConfig.audit.maxErrors)
      .toDF(rejectedCols.map { case (attrName, _, _) => attrName }: _*)

    val taskDesc =
      AutoTaskDesc(
        name = s"metrics-$applicationId-rejected",
        sql = None,
        database = settings.appConfig.audit.getDatabase(),
        domain = settings.appConfig.audit.getDomain(),
        table = "rejected",
        write = Some(WriteMode.APPEND),
        partition = Nil,
        presql = Nil,
        postsql = Nil,
        sink = Some(settings.appConfig.audit.sink),
        parseSQL = Some(false),
        _auditTableName = Some("rejected")
      )
    val autoTask = new SparkAutoTask(
      taskDesc,
      Map.empty,
      None,
      truncate = false
    )
    val res = autoTask.sink(Some(rejectedDF))
    if (res) {
      Success(rejectedDF, rejectedPath)
    } else {
      Failure(new Exception("Failed to save rejected"))
    }
  }

  def validateCol(
    colRawValue: Option[String],
    colAttribute: Attribute,
    tpe: Type,
    colMap: => Map[String, Option[String]],
    allPrivacyLevels: Map[String, ((PrivacyEngine, List[String]), PrivacyLevel)],
    emptyIsNull: Boolean
  ): ColResult = {
    def ltrim(s: String) = s.replaceAll("^\\s+", "")

    def rtrim(s: String) = s.replaceAll("\\s+$", "")

    val trimmedColValue = colRawValue.map { colRawValue =>
      colAttribute.trim match {
        case Some(NONE) | None => colRawValue
        case Some(LEFT)        => ltrim(colRawValue)
        case Some(RIGHT)       => rtrim(colRawValue)
        case Some(BOTH)        => colRawValue.trim()
        case _                 => colRawValue
      }
    }

    val colValue = colAttribute.default match {
      case None =>
        trimmedColValue
      case Some(default) =>
        trimmedColValue
          .map(value => if (value.isEmpty) default else value)
          .orElse(colAttribute.default)
    }

    def colValueIsNullOrEmpty = colValue match {
      case None           => true
      case Some(colValue) => colValue.isEmpty
    }

    def colValueIsNull = colValue.isEmpty

    def optionalColIsEmpty = !colAttribute.required && colValueIsNullOrEmpty

    def requiredColIsEmpty = {
      if (emptyIsNull)
        colAttribute.required && colValueIsNullOrEmpty
      else
        colAttribute.required && colValueIsNull
    }

    def colPatternIsValid = colValue.exists(tpe.matches)

    val privacyLevel = colAttribute.getPrivacy()
    val colValueWithPrivacyApplied =
      if (privacyLevel == PrivacyLevel.None || privacyLevel.sql) {
        colValue
      } else {
        val ((privacyAlgo, privacyParams), _) = allPrivacyLevels(privacyLevel.value)
        colValue.map(colValue => privacyLevel.crypt(colValue, colMap, privacyAlgo, privacyParams))

      }

    val colPatternOK = !requiredColIsEmpty && (optionalColIsEmpty || colPatternIsValid)

    val (sparkValue, colParseOK) = {
      (colPatternOK, colValueWithPrivacyApplied) match {
        case (true, Some(colValueWithPrivacyApplied)) =>
          Try(tpe.sparkValue(colValueWithPrivacyApplied)) match {
            case Success(res) => (Some(res), true)
            case Failure(_)   => (None, false)
          }
        case (colPatternResult, _) =>
          (None, colPatternResult)
      }
    }
    ColResult(
      ColInfo(
        colValue,
        colAttribute.name,
        tpe.name,
        tpe.pattern,
        colPatternOK && colParseOK
      ),
      sparkValue.orNull
    )
  }

  def fastValidateCol(
    colRawValue: Option[String],
    colAttribute: Attribute,
    tpe: Type
  ): ColResult = {
    val sparkValue = colRawValue.map(tpe.sparkValue).orNull
    ColResult(
      ColInfo(
        colRawValue,
        colAttribute.name,
        tpe.name,
        tpe.pattern,
        success = true
      ),
      sparkValue
    )
  }
}
case class NativeBqLoadInfo(
  totalAcceptedRows: Long,
  totalRejectedRows: Long,
  jobResult: BigQueryJobResult
) {
  val totalRows: Long = totalAcceptedRows + totalRejectedRows
}
