package ai.starlake.job.ingest

import ai.starlake.config.{CometColumns, DatasetArea, Settings, SparkSessionBuilder}
import ai.starlake.exceptions.DisallowRejectRecordException
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.validator.SimpleRejectedRecord
import ai.starlake.job.ingest.loaders.{
  BigQueryNativeLoader,
  DuckDbNativeLoader,
  NativeLoader,
  SnowflakeNativeLoader
}
import ai.starlake.job.metrics.*
import ai.starlake.job.sink.bigquery.*
import ai.starlake.job.transform.{SparkAutoTask, SparkExportTask}
import ai.starlake.job.validator.{CheckValidityResult, GenericRowValidator}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.utils.Formatter.*
import ai.starlake.utils.*
import com.google.cloud.bigquery.TableId
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.*
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{StructField, StructType}

import java.sql.Timestamp
import java.time.Instant
import scala.util.{Failure, Success, Try}

trait IngestionJob extends SparkJob {
  val accessToken: Option[String]
  val test: Boolean
  val scheduledDate: Option[String]
  private def loadGenericValidator(validatorClass: String): GenericRowValidator = {
    val validatorClassName = loader.toLowerCase() match {
      case "native" =>
        logger.warn(s"Unexpected '$loader' loader !!!")
        validatorClass
      case "spark" => validatorClass
      case _ =>
        throw new Exception(s"Unexpected '$loader' loader !!!")
    }
    Utils.loadInstance[GenericRowValidator](validatorClassName)
  }

  protected lazy val rowValidator: GenericRowValidator =
    loadGenericValidator(settings.appConfig.rowValidatorClass)

  def domain: DomainInfo

  def schema: SchemaInfo

  def storageHandler: StorageHandler

  def schemaHandler: SchemaHandler

  def types: List[Type]

  def path: List[Path]

  def options: Map[String, String]

  def name: String =
    s"""${domain.name}-${schema.name}-${path.headOption.map(_.getName).mkString(",")}"""

  lazy val strategy: WriteStrategy = {
    val s = mergedMetadata.getStrategyOptions()
    val startTs = s.startTs.getOrElse(settings.appConfig.scd2StartTimestamp)
    val endTs = s.endTs.getOrElse(settings.appConfig.scd2EndTimestamp)
    s.copy(startTs = Some(startTs), endTs = Some(endTs))
  }

  def targetTableName: String = domain.finalName + "." + schema.finalName

  val now: Timestamp = java.sql.Timestamp.from(Instant.now)

  /** Merged metadata
    */
  lazy val mergedMetadata: Metadata = schema.mergedMetadata(domain.metadata)
  lazy val loader: String = mergedMetadata.loader.getOrElse(settings.appConfig.loader)

  private val accessTokenOptions: Map[String, String] =
    accessToken.map(token => Map("gcpAccessToken" -> token)).getOrElse(Map.empty)

  protected val sparkOptions = mergedMetadata.getOptions() ++ accessTokenOptions

  override protected def withExtraSparkConf(sourceConfig: SparkConf): SparkConf = {
    val conf = super.withExtraSparkConf(sourceConfig)
    conf.set("spark.sql.parser.escapedStringLiterals", "true")
  }

  /** Apply the schema to the dataset. This is where all the magic happen Valid records are stored
    * in the accepted path / table and invalid records in the rejected path / table
    *
    * @param dataset
    *   : Spark Dataset
    */
  protected def ingest(dataset: DataFrame): (Dataset[SimpleRejectedRecord], Dataset[Row]) = {
    val validationResult = rowValidator.validate(
      session,
      mergedMetadata.resolveFormat(),
      mergedMetadata.resolveSeparator(),
      dataset,
      schema.attributesWithoutScriptedFieldsWithInputFileName,
      types,
      schema.sourceSparkSchemaWithoutScriptedFieldsWithInputFileName(schemaHandler),
      settings.appConfig.privacy.options,
      settings.appConfig.cacheStorageLevel,
      settings.appConfig.sinkReplayToFile,
      mergedMetadata.emptyIsNull.getOrElse(settings.appConfig.emptyIsNull),
      settings.appConfig.rejectWithValue
    )(schemaHandler)

    saveRejected(
      validationResult.errors,
      validationResult.rejected.drop(CometColumns.cometInputFileNameColumn)
    )(
      settings,
      storageHandler,
      schemaHandler
    ).flatMap { _ =>
      saveAccepted(validationResult) // prefer to let Spark compute the final schema
    } match {
      case Failure(exception) =>
        throw exception
      case Success(_) => // rejectedRecordCount is always 0 in saveAccepted
        (validationResult.errors, validationResult.accepted);
    }
  }

  protected def reorderTypes(orderedAttributes: List[TableAttribute]): (List[Type], StructType) = {
    val typeMap: Map[String, Type] = types.map(tpe => tpe.name -> tpe).toMap
    val (tpes, sparkFields) = orderedAttributes.map { attribute =>
      val tpe = typeMap(attribute.`type`)
      (tpe, tpe.sparkType(attribute.name, !attribute.resolveRequired(), attribute.comment))
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

  private def extractHiveTableAcl(): List[String] = {

    if (settings.appConfig.isHiveCompatible()) {
      val fullTableName = schemaHandler.getFullTableName(domain, schema)
      schema.acl.flatMap { ace =>
        ace.asSql(fullTableName, engine = Engine.SPARK)
      }
    } else {
      Nil
    }
  }

  def applyHiveTableAcl(): Try[Unit] =
    Try {
      val sqls = extractHiveTableAcl()
      sqls.foreach { sql =>
        SparkUtils.sql(session, sql)
      }
    }

  def applyJdbcAcl(connection: Settings.ConnectionInfo, forceApply: Boolean = false): Try[Unit] = {
    val fullTableName = schemaHandler.getFullTableName(domain, schema)
    val sqls =
      schema.acl.flatMap { ace =>
        ace.asSql(fullTableName, connection.getJdbcEngineName())
      }
    AccessControlEntry.applyJdbcAcl(
      connection,
      sqls,
      forceApply
    )
  }

  private def bqNativeJob(tableId: TableId, sql: String)(implicit settings: Settings) = {
    val bqConfig = BigQueryLoadConfig(
      connectionRef = Some(mergedMetadata.getSinkConnectionRef()),
      outputDatabase = schemaHandler.getDatabase(domain),
      outputTableId = Some(tableId),
      accessToken = accessToken
    )
    new BigQueryNativeJob(bqConfig, sql)
  }

  def runPrePostSql(engine: Engine, sqls: List[String]): Unit = {
    engine match {

      case Engine.BQ =>
        val connection = settings.appConfig.connections(mergedMetadata.getSinkConnectionRef())
        val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
          schemaHandler.getDatabase(domain),
          domain.finalName,
          schema.finalName,
          connection.options.get("projectId").orElse(settings.appConfig.getDefaultDatabase())
        )

        sqls.foreach { sql =>
          val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
          bqNativeJob(tableId, compiledSql).runBigQueryJob()
        }

      case Engine.JDBC =>
        val connection = settings.appConfig.connections(mergedMetadata.getSinkConnectionRef())
        JdbcDbUtils.withJDBCConnection(
          this.schemaHandler.dataBranch(),
          connection.withAccessToken(accessToken).options
        ) { conn =>
          sqls.foreach { sql =>
            val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
            JdbcDbUtils.executeUpdate(compiledSql, conn)
          }
        }

      case _ =>
        val exists = SparkUtils.tableExists(session, targetTableName)
        if (exists)
          session.sql(s"select * from $targetTableName").createOrReplaceTempView("SL_THIS")
        sqls.foreach { sql =>
          val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
          SparkUtils.sql(session, compiledSql)
        }
    }
  }

  private def selectLoader(): String = {
    val sinkConn = mergedMetadata.getSinkConnection()
    val dbName = sinkConn.targetDatawareHouse()
    val nativeCandidate: Boolean = isNativeCandidate(dbName)
    logger.info(s"Native candidate: $nativeCandidate")

    val loader =
      if (nativeCandidate) {
        val loaders = Set("bigquery", "duckdb", "spark", "snowflake")
        if (loaders.contains(dbName))
          dbName
        else
          "spark"
      } else {
        "spark"
      }
    logger.info(s"Using $loader as ingestion engine")
    loader
  }

  private def isNativeCandidate(dbName: String): Boolean = {
    val nativeValidator =
      mergedMetadata.loader
        .orElse(mergedMetadata.getSinkConnection().loader)
        .getOrElse(settings.appConfig.loader)
        .toLowerCase()
        .equals("native")
    if (!nativeValidator) {
      false
    } else {
      dbName match {
        case "bigquery" =>
          val csvOrJsonLines =
            !mergedMetadata.resolveArray() && Set(Format.DSV, Format.JSON, Format.JSON_FLAT)
              .contains(
                mergedMetadata.resolveFormat()
              )
          // https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
          csvOrJsonLines
        case "duckdb" =>
          Set(Format.DSV, Format.JSON, Format.JSON_FLAT)
            .contains(
              mergedMetadata.resolveFormat()
            )
        case "snowflake" =>
          Set(Format.DSV, Format.JSON, Format.JSON_FLAT, Format.XML, Format.PARQUET)
            .contains(
              mergedMetadata.resolveFormat()
            )
        case "redshift" =>
          Set(Format.DSV, Format.JSON, Format.JSON_FLAT)
            .contains(
              mergedMetadata.resolveFormat()
            )
        case _ => false
      }
    }
  }

  def logLoadFailureInAudit(start: Timestamp, exception: Throwable): Failure[Nothing] = {
    exception.printStackTrace()
    val end = Timestamp.from(Instant.now())
    val err = Utils.exceptionAsString(exception)
    val logs = if (settings.appConfig.audit.detailedLoadAudit) {
      // create duplicated log entry for each entry path because we currently don't know which job fails at this step.
      // need more rework to target path that fails.
      // splitting by path allows to reduce size of one log entry or query which is what we are aiming at with
      // detailed load audit
      path.map { p =>
        AuditLog(
          applicationId(),
          Some(p.toString),
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
          settings.appConfig.tenant,
          test = false,
          scheduledDate
        )
      }
    } else {
      List(
        AuditLog(
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
          settings.appConfig.tenant,
          test = false,
          scheduledDate
        )
      )
    }
    AuditLog.sink(logs, accessToken)(settings, storageHandler, schemaHandler)
    logger.error(err)
    Failure(exception)
  }

  def logLoadInAudit(
    start: Timestamp,
    ingestionCounters: List[IngestionCounters]
  ): Try[List[AuditLog]] = {
    val logs = ingestionCounters.map { counter =>
      val inputFiles = counter.paths.mkString(",")
      logger.info(
        s"ingestion-summary -> files: [$inputFiles], domain: ${domain.name}, schema: ${schema.name}, input: ${counter.inputCount}, accepted: ${counter.acceptedCount}, rejected:${counter.rejectedCount}"
      )
      val end = Timestamp.from(Instant.now())
      val success = !settings.appConfig.rejectAllOnError || counter.rejectedCount == 0
      AuditLog(
        applicationId(),
        Some(inputFiles),
        domain.name,
        schema.name,
        success = success,
        counter.inputCount,
        counter.acceptedCount,
        counter.rejectedCount,
        start,
        end.getTime - start.getTime,
        if (success) "success" else s"${counter.rejectedCount} invalid records",
        Step.LOAD.toString,
        schemaHandler.getDatabase(domain),
        settings.appConfig.tenant,
        test = false,
        scheduledDate
      )
    }
    AuditLog.sink(logs, accessToken)(settings, storageHandler, schemaHandler).map(_ => logs)
  }

  @throws[Exception]
  private def checkDomainValidity(): Unit = {
    domain.checkValidity(schemaHandler) match {
      case Left(errors) =>
        val errs = errors.map(_.toString()).reduce { (errs, err) =>
          errs + "\n" + err
        }
        throw new Exception(s"-- $name --\n" + errs)
      case Right(_) =>
    }
  }
  def buildListOfSQLStatementsAsMap(orchestrator: String): Map[String, Object] = {
    // Run selected ingestion engine
    val result =
      orchestrator match {
        case "bigquery" =>
          ???
        case "duckdb" =>
          ???
        case "spark" =>
          ???
        case "snowflake" =>
          val statementsMap = new NativeLoader(this, None).buildSQLStatements()
          statementsMap
        case other =>
          throw new Exception(s"Unsupported engine $other")
      }
    result
  }

  def run(): Try[JobResult] = {
    // Make sure domain is valid
    // checkDomainValidity()
    val engineName = this.mergedMetadata.getSinkConnection().getJdbcEngineName()
    val engine = settings.appConfig.jdbcEngines
      .getOrElse(engineName.toString, "spark")

    // Run selected ingestion engine
    val jobResult = selectLoader() match {
      case "bigquery" =>
        val ingestionCounters = new BigQueryNativeLoader(this, accessToken).run()
        ingestionCounters
      case "snowflake" =>
        val ingestionCounters = new SnowflakeNativeLoader(this).run()
        ingestionCounters
      case "duckdb" =>
        val ingestionCounters = new DuckDbNativeLoader(this).run()
        ingestionCounters
      case "spark" => // databricks
        val result = ingestWithSpark()
        result
      case other =>
        logger.warn(s"Unsupported engine $other, falling back to spark")
        val result = ingestWithSpark()
        result
    }
    jobResult
      .recoverWith { case exception =>
        // on failure log failures
        logLoadFailureInAudit(now, exception)
      }
      .map { (counterResults: List[IngestionCounters]) =>
        val validCounterResults = counterResults.filter(!_.ignore)
        logLoadInAudit(now, validCounterResults) match {
          case Failure(exception) => throw exception
          case Success(auditLogs) =>
            if (auditLogs.exists(!_.success)) {
              throw new DisallowRejectRecordException()
            }
        }
        // run expectations
        val expectationsResult = runExpectations()
        expectationsResult match {
          case Failure(exception) if settings.appConfig.expectations.failOnError =>
            throw exception
          case _ =>
        }
        val globalCounters: Option[IngestionCounters] =
          validCounterResults.foldLeft[Option[IngestionCounters]](None) {
            case (
                  cumulatedCounters,
                  counters @ IngestionCounters(
                    inputCount,
                    acceptedCount,
                    rejectedCount,
                    paths,
                    jobid
                  )
                ) =>
              cumulatedCounters
                .map { cc =>
                  IngestionCounters(
                    inputCount = cc.inputCount + inputCount,
                    acceptedCount = cc.acceptedCount + acceptedCount,
                    rejectedCount = cc.rejectedCount + rejectedCount,
                    paths = cc.paths ++ paths,
                    jobid = this.applicationId() + "," + jobid
                  )

                }
                .orElse(Some(counters))
          }
        SparkJobResult(
          None,
          globalCounters.orElse(
            Some(
              IngestionCounters(
                inputCount = -1,
                acceptedCount = -1,
                rejectedCount = -1,
                paths = path.map(_.toString),
                jobid = applicationId()
              )
            )
          )
        )
      }
  }

  ///////////////////////////////////////////////////////////////////////////
  /////// SPARK ENGINE ONLY /////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////

  /** Main entry point as required by the Spark Job interface
    *
    * @return
    *   : Spark Session used for the job
    */
  def ingestWithSpark(): Try[List[IngestionCounters]] = {
    if (!SparkSessionBuilder.isSparkConnectActive) {
      session.sparkContext.setLocalProperty(
        "spark.scheduler.pool",
        settings.appConfig.sparkScheduling.poolName
      )
    }
    val jobResult = {
      val start = Timestamp.from(Instant.now())
      val dataset = loadDataSet()
      dataset match {
        case Success(dataset) =>
          Try {
            val (rejectedDS, acceptedDS) = ingest(dataset)
            if (settings.appConfig.audit.detailedLoadAudit && path.size > 1) {
              import session.implicits._
              rejectedDS
                .groupBy("path")
                .count()
                .withColumnRenamed("count", "rejectedCount")
                .join(
                  acceptedDS
                    .groupBy(col(CometColumns.cometInputFileNameColumn).as("path"))
                    .count()
                    .withColumnRenamed("count", "acceptedCount"),
                  "path",
                  "full_outer"
                )
                .select(
                  array(col("path")).as("paths"),
                  coalesce(col("rejectedCount"), lit(0L)).as("rejectedCount"),
                  coalesce(col("acceptedCount"), lit(0L)).as("acceptedCount")
                )
                .withColumn("inputCount", col("rejectedCount") + col("acceptedCount"))
                .withColumn("jobid", lit(applicationId()))
                .as[IngestionCounters]
                .collect()
                .toList
            } else {
              val totalAcceptedCount = acceptedDS.count()
              val totalRejectedCount = rejectedDS.count()
              List(
                IngestionCounters(
                  inputCount = totalAcceptedCount + totalRejectedCount,
                  acceptedCount = totalAcceptedCount,
                  rejectedCount = totalRejectedCount,
                  paths = path.map(_.toString),
                  jobid = applicationId()
                )
              )
            }
          }
        case Failure(exception) =>
          logLoadFailureInAudit(start, exception)
      }
    }
    // After each ingestion job we explicitly clear the spark cache
    session.catalog.clearCache()
    jobResult
  }

  // /////////////////////////////////////////////////////////////////////////
  // endregion
  // /////////////////////////////////////////////////////////////////////////

  def reorderAttributes(dataFrame: DataFrame): List[TableAttribute] = {
    val finalSchema = schema.attributesWithoutScriptedFields :+ TableAttribute(
      name = CometColumns.cometInputFileNameColumn
    )
    val attributesMap =
      finalSchema.map(attr => (attr.name, attr)).toMap
    dataFrame.columns.map(colName => attributesMap(colName)).toList
  }

  private def runExpectations(): Try[JobResult] = {
    mergedMetadata.getSink() match {
      case _: BigQuerySink =>
        val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
          settings.appConfig.audit.getDatabase(),
          settings.appConfig.audit.getDomain(),
          "expectations",
          mergedMetadata
            .getSinkConnection()
            .options
            .get("projectId")
            .orElse(settings.appConfig.getDefaultDatabase())
        )
        runBigQueryExpectations(bqNativeJob(tableId, ""))
      case _: JdbcSink =>
        val options = mergedMetadata.getSinkConnection().withAccessToken(accessToken).options
        runJdbcExpectations(options)
      case _ =>
        runSparkExpectations(session)
    }
  }
  private def runJdbcExpectations(
    jdbcOptions: Map[String, String]
  ): Try[JobResult] = {
    if (settings.appConfig.expectations.active) {

      new ExpectationJob(
        Option(applicationId()),
        schemaHandler.getDatabase(this.domain),
        this.domain.finalName,
        this.schema.finalName,
        this.schema.expectations,
        storageHandler,
        schemaHandler,
        new JdbcExpectationAssertionHandler(jdbcOptions),
        false
      ).run()
    } else {
      Success(SparkJobResult(None, None))
    }
  }

  private def runSparkExpectations(session: SparkSession): Try[JobResult] = {
    if (settings.appConfig.expectations.active) {
      new ExpectationJob(
        Option(applicationId()),
        schemaHandler.getDatabase(this.domain),
        this.domain.finalName,
        this.schema.finalName,
        this.schema.expectations,
        storageHandler,
        schemaHandler,
        new SparkExpectationAssertionHandler(session),
        false
      ).run()
    } else {
      Success(SparkJobResult(None, None))
    }
  }

  def runBigQueryExpectations(
    job: BigQueryNativeJob
  ): Try[JobResult] = {
    if (settings.appConfig.expectations.active) {
      new ExpectationJob(
        Option(applicationId()),
        schemaHandler.getDatabase(this.domain),
        this.domain.finalName,
        this.schema.finalName,
        this.schema.expectations,
        storageHandler,
        schemaHandler,
        new BigQueryExpectationAssertionHandler(job),
        false
      ).run()
    } else {
      Success(SparkJobResult(None, None))
    }
  }

  private def runMetrics(acceptedDF: DataFrame) = {
    if (settings.appConfig.metrics.active) {
      new MetricsJob(
        Option(applicationId()),
        this.domain,
        this.schema,
        this.storageHandler,
        this.schemaHandler
      )
        .run(acceptedDF, System.currentTimeMillis())
    }
  }

  def dfWithAttributesRenamed(acceptedDF: DataFrame): DataFrame = {
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
    validationResult: CheckValidityResult
  ): Try[Long] = {
    if (!settings.appConfig.rejectAllOnError || validationResult.rejected.isEmpty) {
      logger.whenDebugEnabled {
        logger.debug(s"accepted SIZE ${validationResult.accepted.count()}")
        logger.debug(validationResult.accepted.showString(1000))
      }

      val finalAcceptedDF = computeFinalDF(validationResult.accepted)
      sinkAccepted(finalAcceptedDF)
        .map { rejectedRecordCount =>
          runMetrics(finalAcceptedDF)
          rejectedRecordCount
        }
    } else {
      Success(0)
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
    val ignoredAttributes = schema.attributes.filter(_.resolveIgnore()).map(_.getFinalName())
    val acceptedDfWithoutIgnoredFields =
      acceptedDfWithScriptAndTransformedFields.drop(ignoredAttributes: _*)
    acceptedDfWithoutIgnoredFields
  }

  private def computeTransformedAttributes(acceptedDfWithScriptFields: DataFrame): DataFrame = {
    val sqlAttributes =
      schema.attributes.filter(_.resolvePrivacy().sql).filter(_.transform.isDefined)
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
    def enrichStructField(attr: TableAttribute, structField: StructField) = {
      structField.copy(
        name = attr.getFinalName(),
        nullable = if (attr.script.isDefined) true else !attr.resolveRequired(),
        metadata =
          if (attr.`type` == "variant")
            org.apache.spark.sql.types.Metadata.fromJson("""{ "sqlType" : "JSON"}""")
          else org.apache.spark.sql.types.Metadata.empty
      )
    }
    schema.attributes
      .filter(_.script.isDefined)
      .map(attr =>
        (
          attr.getFinalName(),
          attr.sparkType(schemaHandler, enrichStructField),
          attr.resolveScript()
        )
      )
      .foldLeft(acceptedDF) { case (df, (name, sparkType, script)) =>
        df.withColumn(
          name,
          expr(script.richFormat(schemaHandler.activeEnvVars(), options))
            .cast(sparkType)
        )
      }
  }

  private def sinkAccepted(mergedDF: DataFrame): Try[Long] = {
    val result: Try[Try[Long]] = Try {
      val taskDesc = AutoTaskInfo(
        name = schema.finalName,
        presql = schema.presql,
        postsql = schema.postsql,
        sql = None,
        database = schemaHandler.getDatabase(domain),
        domain = domain.finalName,
        table = schema.finalName,
        sink = mergedMetadata.sink,
        acl = schema.acl,
        rls = schema.rls,
        comment = schema.comment,
        tags = schema.tags,
        writeStrategy = Some(strategy),
        connectionRef = Option(mergedMetadata.getSinkConnectionRef())
      )
      val autoTask =
        taskDesc.getSinkConfig() match {
          case fsSink: FsSink if fsSink.isExport() && !strategy.isMerge() =>
            new SparkExportTask(
              appId = Option(applicationId()),
              taskDesc = taskDesc,
              commandParameters = Map.empty,
              interactive = None,
              truncate = false,
              test = test,
              logExecution = false,
              resultPageSize = 200,
              resultPageNumber = 1,
              scheduledDate = scheduledDate
            )(
              settings,
              storageHandler,
              schemaHandler
            )
          case _ =>
            new SparkAutoTask(
              appId = Option(applicationId()),
              taskDesc = taskDesc,
              commandParameters = Map.empty,
              interactive = None,
              truncate = false,
              test = test,
              logExecution = false,
              schema = Some(schema),
              accessToken = accessToken,
              resultPageSize = 200,
              resultPageNumber = 1,
              scheduledDate = scheduledDate,
              syncSchema = false
            )(
              settings,
              storageHandler,
              schemaHandler
            )
        }
      if (autoTask.sink(mergedDF, Some(this.schema))) {
        Success(0L)
      } else {
        Failure(new Exception("Failed to sink"))
      }
    }
    result.flatten
  }

  def loadDataSet(): Try[DataFrame]

  def defineOutputAsOriginalFormat(rejectedLines: DataFrame): DataFrameWriter[Row]

  protected def saveRejected(
    errMessagesDS: Dataset[SimpleRejectedRecord],
    rejectedLinesDS: DataFrame
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[Path] = {
    logger.whenDebugEnabled {
      logger.debug(s"rejected SIZE ${errMessagesDS.count()}")
      errMessagesDS
        .take(100)
        .foreach(rejected => logger.debug(rejected.errors.replaceAll("\n", "|")))
    }
    val domainName = domain.name
    val schemaName = schema.name

    val start = Timestamp.from(Instant.now())
    val formattedDate = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(start)

    if (settings.appConfig.sinkReplayToFile && !rejectedLinesDS.isEmpty) {
      val replayArea = DatasetArea.replay(domainName)
      val targetPath =
        new Path(replayArea, s"$domainName.$schemaName.$formattedDate.replay")
      val formattedRejectedLinesDF: DataFrameWriter[Row] = defineOutputAsOriginalFormat(
        rejectedLinesDS.repartition(1)
      )
      formattedRejectedLinesDF
        .save(targetPath.toString)
      storageHandler.moveSparkPartFile(
        targetPath,
        "0000" // When saving as text file, no extension is added.
      )
    }

    IngestionUtil.sinkRejected(
      applicationId(),
      session,
      errMessagesDS,
      domainName,
      schemaName,
      now,
      path,
      scheduledDate
    ) match {
      case Success((rejectedDF, rejectedPath)) =>
        Success(rejectedPath)
      case Failure(exception) =>
        logger.error("Failed to save Rejected", exception)
        Failure(exception)
    }
  }
}
