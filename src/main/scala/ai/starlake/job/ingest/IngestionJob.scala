package ai.starlake.job.ingest

import ai.starlake.config.{CometColumns, DatasetArea, Settings}
import ai.starlake.exceptions.DisallowRejectRecordException
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.loaders.{BigQueryNativeLoader, DuckDbNativeLoader}
import ai.starlake.job.metrics._
import ai.starlake.job.sink.bigquery._
import ai.starlake.job.transform.{SparkAutoTask, SparkExportTask}
import ai.starlake.job.validator.{CheckValidityResult, GenericRowValidator}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.utils._
import com.google.cloud.bigquery.TableId
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

import java.sql.Timestamp
import java.time.Instant
import scala.annotation.nowarn
import scala.util.{Failure, Success, Try}

trait IngestionJob extends SparkJob {
  val accessToken: Option[String]
  val test: Boolean
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

  /** ingestion algorithm
    *
    * @param dataset
    */
  protected def ingest(dataset: DataFrame): (Dataset[String], Dataset[Row], Long)

  protected def reorderTypes(orderedAttributes: List[Attribute]): (List[Type], StructType) = {
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
          SparkUtils.sql(session, sql)
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
        val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
          schemaHandler.getDatabase(domain),
          domain.finalName,
          schema.finalName
        )

        sqls.foreach { sql =>
          val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
          bqNativeJob(tableId, compiledSql).runInteractiveQuery()
        }

      case Engine.JDBC =>
        val connection = settings.appConfig.connections(mergedMetadata.getSinkConnectionRef())
        JdbcDbUtils.withJDBCConnection(connection.options) { conn =>
          sqls.foreach { sql =>
            val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
            JdbcDbUtils.executeUpdate(compiledSql, conn)
          }
        }

      case _ =>
        if (session.catalog.tableExists(s"$targetTableName"))
          session.sql(s"select * from $targetTableName").createOrReplaceTempView("SL_THIS")
        sqls.foreach { sql =>
          val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
          SparkUtils.sql(session, compiledSql)
        }
    }
  }

  private def selectLoader(): String = {
    val sinkConn = mergedMetadata.getSinkConnection()
    val dbName = sinkConn.getDbName()
    val nativeCandidate: Boolean = isNativeCandidate(dbName)

    val loader =
      if (nativeCandidate) {
        val loaders = Set("bigquery", "duckdb", "spark")
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

  private def isNativeCandidate(dbType: String): Boolean = {
    val nativeValidator =
      mergedMetadata.loader
        .orElse(mergedMetadata.getSinkConnection().loader)
        .getOrElse(settings.appConfig.loader)
        .toLowerCase()
        .equals("native")
    if (!nativeValidator) {
      false
    } else {
      dbType match {
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
          Set(Format.DSV, Format.JSON, Format.JSON_FLAT)
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
      settings.appConfig.tenant,
      false
    )
    AuditLog.sink(log)(settings, storageHandler, schemaHandler)
    logger.error(err)
    Failure(exception)
  }

  def logLoadInAudit(
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
      settings.appConfig.tenant,
      test = false
    )
    AuditLog.sink(log)(settings, storageHandler, schemaHandler).map(_ => log)
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

  def run(): Try[JobResult] = {
    // Make sure domain is valid
    // checkDomainValidity()
    val engineName = this.mergedMetadata.getSinkConnection().getJdbcEngineName()
    val engine = settings.appConfig.jdbcEngines
      .getOrElse(engineName.toString, throw new Exception(s"Unknown engine $engineName"))

    // Run selected ingestion engine
    val jobResult = selectLoader() match {
      case "bigquery" =>
        val ingestionCounters = new BigQueryNativeLoader(this, accessToken).run()
        ingestionCounters
      case "duckdb" =>
        val ingestionCounters = new DuckDbNativeLoader(this).run()
        ingestionCounters
      case "spark" =>
        val result = ingestWithSpark()
        result
      case other =>
        throw new Exception(s"Unsupported engine $other")
    }
    jobResult
      .recoverWith { case exception =>
        // on failure log failures
        logLoadFailureInAudit(now, exception)
      }
      .map { case counters @ IngestionCounters(inputCount, acceptedCount, rejectedCount) =>
        // On success log counters
        logLoadInAudit(now, inputCount, acceptedCount, rejectedCount) match {
          case Failure(exception) => throw exception
          case Success(auditLog) =>
            if (auditLog.success) {
              // run expectations
              val expectationsResult = runExpectations()

              expectationsResult match {
                case Failure(exception) if settings.appConfig.expectations.failOnError =>
                  throw exception
                case _ =>
              }
              SparkJobResult(None, Some(counters))
            } else throw new DisallowRejectRecordException()
        }
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
  def ingestWithSpark(): Try[IngestionCounters] = {
    session.sparkContext.setLocalProperty(
      "spark.scheduler.pool",
      settings.appConfig.sparkScheduling.poolName
    )
    val jobResult = {
      val start = Timestamp.from(Instant.now())
      val dataset = loadDataSet(false)
      dataset match {
        case Success(dataset) =>
          Try {
            val (rejectedDS, acceptedDS, rejectedCount) = ingest(dataset)
            val inputCount = dataset.count()
            val totalAcceptedCount = acceptedDS.count() - rejectedCount
            val totalRejectedCount = rejectedDS.count() + rejectedCount
            IngestionCounters(inputCount, totalAcceptedCount, totalRejectedCount)
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

  def reorderAttributes(dataFrame: DataFrame): List[Attribute] = {
    val finalSchema = schema.attributesWithoutScriptedFields :+ Attribute(
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
          "expectations"
        )
        runBigQueryExpectations(bqNativeJob(tableId, ""))
      case _: JdbcSink =>
        val options = mergedMetadata.getSinkConnection().options
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
        new JdbcExpectationAssertionHandler(jdbcOptions)
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
        new SparkExpectationAssertionHandler(session)
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
        new BigQueryExpectationAssertionHandler(job)
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

  private def dfWithSCD2Columns(df: DataFrame): DataFrame = {
    val finalDF =
      df.withColumn(settings.appConfig.scd2StartTimestamp, lit(null: Timestamp))
        .withColumn(settings.appConfig.scd2EndTimestamp, lit(null: Timestamp))
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
        logger.debug(s"acceptedRDD SIZE ${validationResult.accepted.count()}")
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
    def enrichStructField(attr: Attribute, structField: StructField) = {
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
      val taskDesc = AutoTaskDesc(
        name = schema.finalName,
        presql = schema.presql,
        postsql = schema.postsql,
        sql = None,
        database = schemaHandler.getDatabase(domain),
        domain = domain.finalName,
        table = schema.finalName,
        sink = mergedMetadata.sink,
        acl = schema.acl,
        comment = schema.comment,
        tags = schema.tags,
        writeStrategy = Some(strategy),
        connectionRef = Option(mergedMetadata.getSinkConnectionRef())
      )
      val autoTask =
        taskDesc.getSinkConfig() match {
          case fsSink: FsSink if fsSink.isExport() && !strategy.isMerge() =>
            new SparkExportTask(
              Option(applicationId()),
              taskDesc,
              Map.empty,
              None,
              truncate = false,
              test = test,
              logExecution = true
            )(
              settings,
              storageHandler,
              schemaHandler
            )
          case _ =>
            new SparkAutoTask(
              Option(applicationId()),
              taskDesc,
              Map.empty,
              None,
              truncate = false,
              test = test,
              logExecution = true,
              schema = Some(schema)
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

  def loadDataSet(withSchema: Boolean): Try[DataFrame]

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
      applicationId(),
      session,
      errMessagesDS,
      domainName,
      schemaName,
      now,
      path
    ) match {
      case Success((rejectedDF, rejectedPath)) =>
        Success(rejectedPath)
      case Failure(exception) =>
        logger.error("Failed to save Rejected", exception)
        Failure(exception)
    }
  }
}
