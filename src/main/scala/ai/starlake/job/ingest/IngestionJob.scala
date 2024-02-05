package ai.starlake.job.ingest

import ai.starlake.config.{CometColumns, DatasetArea, Settings}
import ai.starlake.exceptions.DisallowRejectRecordException
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.metrics._
import ai.starlake.job.sink.bigquery._
import ai.starlake.job.sink.es.{ESLoadConfig, ESLoadJob}
import ai.starlake.job.transform.SparkAutoTask
import ai.starlake.job.validator.{GenericRowValidator, ValidationResult}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.utils._
import ai.starlake.utils.conversion.BigQueryUtils
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.{
  Field,
  LegacySQLTypeName,
  Schema => BQSchema,
  StandardTableDefinition,
  TableId
}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.time.Instant
import scala.annotation.nowarn
import scala.jdk.CollectionConverters.{asJavaIterableConverter, asScalaBufferConverter}
import scala.util.{Failure, Success, Try}

case class IngestionCounters(inputCount: Long, acceptedCount: Long, rejectedCount: Long)

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

  lazy val strategy: StrategyOptions = {
    val s = schema.getStrategy(Some(mergedMetadata))
    val startTs = s.start_ts.getOrElse(settings.appConfig.scd2StartTimestamp)
    val endTs = s.end_ts.getOrElse(settings.appConfig.scd2EndTimestamp)
    s.copy(start_ts = Some(startTs), end_ts = Some(endTs))
  }

  def targetTableName: String = domain.finalName + "." + schema.finalName

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

  def getSinkConnectionType(): ConnectionType = {
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
      outputTableId = Some(tableId)
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

  private def selectLoadEngine(): Engine = {
    val nativeCandidate: Boolean = isNativeCandidate()

    val engine = mergedMetadata.getEngine()

    if (nativeCandidate && engine == Engine.BQ) {
      logger.info("Using BQ as ingestion engine")
      Engine.BQ
    } else if (engine == Engine.JDBC) {
      logger.info("Using Spark for JDBC as ingestion engine")
      Engine.SPARK
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
      settings.appConfig.tenant
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
      settings.appConfig.tenant
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
    checkDomainValidity()

    // run presql
    runPrePostSql(mergedMetadata.getEngine(), schema.presql)

    // Run selected ingestion engine
    val jobResult = selectLoadEngine() match {
      case Engine.BQ =>
        val result = new BigQueryNativeIngestionJob(this).run()
        result
      case Engine.SPARK =>
        val result = runSpark()
        result
      case other =>
        throw new Exception(s"Unsupported engine $other")
    }
    jobResult
      .recoverWith { case exception =>
        // on failure log failures
        logLoadFailureInAudit(now, exception)
      }
      .map { jobResult =>
        // on success run post sql
        runPrePostSql(mergedMetadata.getEngine(), schema.postsql)
        jobResult
      }
      .map { case counters @ IngestionCounters(inputCount, acceptedCount, rejectedCount) =>
        // On success log counters
        logLoadInAudit(now, inputCount, acceptedCount, rejectedCount) match {
          case Failure(exception) => throw exception
          case Success(auditLog) =>
            if (auditLog.success) {
              // run expectations
              val expectationsResult = runExpectations()
              if (expectationsResult.isFailure && settings.appConfig.expectations.failOnError)
                throw new Exception("Expectations failed")
              SparkJobResult(None)
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
  def runSpark(): Try[IngestionCounters] = {
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
  // region Merge between the target and the source Dataframe
  // /////////////////////////////////////////////////////////////////////////

  /** In the queryFilter, the user may now write something like this : `partitionField in last(3)`
    * this will be translated to partitionField between partitionStart and partitionEnd
    *
    * partitionEnd is the last partition in the dataset paritionStart is the 3rd last partition in
    * the dataset
    *
    * if partititionStart or partitionEnd does nos exist (aka empty dataset) they are replaced by
    * 19700101
    *
    * @return
    */
  private def updateBigQueryTableSchema(bigqueryJob: BigQueryNativeJob): Unit = {
    // When merging to BigQuery, load existing DF from BigQuery
    val bqTable = s"${domain.finalName}.${schema.finalName}"
    val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
      schemaHandler.getDatabase(this.domain),
      this.domain.finalName,
      this.schema.finalName
    )
    val tableExists = bigqueryJob.tableExists(tableId)
    val isSCD2 = strategy.`type` == StrategyType.SCD2
    def bqSchemaWithSCD2(incomingTableSchema: BQSchema) = {
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
          val finalSparkSchema = BigQueryUtils.normalizeCompatibleSchema(
            schema.sparkSchemaWithoutIgnore(schemaHandler),
            existingSchema
          )
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
      val bqSchema = schema.bqSchemaWithoutIgnore(schemaHandler)
      val sink = mergedMetadata.getSink().asInstanceOf[BigQuerySink]

      val partitionField = sink.timestamp.map { partitionField =>
        FieldPartitionInfo(partitionField, sink.days, sink.requirePartitionFilter.getOrElse(false))
      }
      val clusteringFields = sink.clustering.flatMap { fields =>
        Some(ClusteringInfo(fields.toList))
      }
      val newSchema = bqSchemaWithSCD2(bqSchema)
      val tableInfo = TableInfo(
        tableId,
        schema.comment,
        Some(newSchema),
        partitionField,
        clusteringFields
      )
      bigqueryJob.getOrCreateTable(domain.comment, tableInfo, None)
    }
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

  /*
  private def sinkToFile(
                          dataset: DataFrame,
                          targetPath: Path,
                          area: StorageArea,
                          strategy: StrategyOptions,
                          writeFormat: String
                        ): Unit = {
      if (csvOutput() && area == StorageArea.accepted) {
        val outputList = storageHandler
          .list(targetPath, ".csv", LocalDateTime.MIN, recursive = false)
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
    }
  }
   */
  private def nbFsPartitions(
    dataset: DataFrame
  ): Int = {
    if (dataset.rdd.getNumPartitions == 0) // avoid error for an empty dataset
      1
    else
      dataset.rdd.getNumPartitions
  }

  private def runExpectations(): Try[JobResult] = {
    mergedMetadata.getSink() match {
      case _: BigQuerySink =>
        val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
          settings.appConfig.audit.getDatabase(),
          settings.appConfig.audit.getDomain(),
          "expectations"
        )
        runExpectations(bqNativeJob(tableId, ""))
      case _: JdbcSink =>
        val options = mergedMetadata.getSinkConnectionRefOptions()
        JdbcDbUtils.withJDBCConnection(options) { conn =>
          runExpectations(conn)
        }
      case _ =>
        runExpectations(session)
    }
  }
  private def runExpectations(
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
        new JdbcExpectationAssertionHandler(connection)
      ).run()
    } else {
      Success(SparkJobResult(None))
    }
  }

  private def runExpectations(session: SparkSession): Try[JobResult] = {
    if (settings.appConfig.expectations.active) {
      new ExpectationJob(
        schemaHandler.getDatabase(this.domain),
        this.domain.finalName,
        this.schema.finalName,
        this.schema.expectations,
        storageHandler,
        schemaHandler,
        new SparkExpectationAssertionHandler(session)
      ).run()
    } else {
      Success(SparkJobResult(None))
    }
  }

  def runExpectations(
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
    validationResult: ValidationResult
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

  private def sinkToES(mergedDF: DataFrame, sink: EsSink): Try[DataFrame] = Try {
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
        write = Some(mergedMetadata.getWrite()),
        sink = mergedMetadata.sink,
        acl = schema.acl,
        comment = schema.comment,
        tags = schema.tags,
        strategy = Some(strategy)
      )
      val autoTask = new SparkAutoTask(taskDesc, Map.empty, None, false)(
        settings,
        storageHandler,
        schemaHandler
      )
      if (autoTask.sink(Some(mergedDF))) {
        Success(0L)
      } else {
        Failure(new Exception("Failed to sink"))
      }
    }
    result.flatten
  }

  /*
  private def bqSinkOneStep(
    loadedDF: DataFrame,
    sink: BigQuerySink,
    createDisposition: String,
    writeDisposition: String,
    outputTableId: TableId
  ): Try[JobResult] = {
    /* We load the schema from the postsql returned dataframe if any */
    val tableSchema = Some(schema.bqSchemaWithoutIgnore(schemaHandler))
    /*
    schema.postsql match {
      case Nil => Some(schema.bqSchemaWithoutIgnore(schemaHandler))
      case _   => Some(BigQueryUtils.bqSchema(mergedDF.schema))
    }
   */
    val config = BigQueryLoadConfig(
      connectionRef = Some(mergedMetadata.getSinkConnectionRef()),
      source = Right(loadedDF),
      outputTableId = Some(outputTableId),
      sourceFormat = settings.appConfig.defaultWriteFormat,
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      outputPartition = sink.timestamp,
      outputClustering = sink.clustering.getOrElse(Nil),
      days = sink.days,
      requirePartitionFilter = sink.requirePartitionFilter.getOrElse(false),
      rls = schema.rls,
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
    res
  }
   */
  def buildCommonNativeBQLoadConfig(
    createDisposition: String,
    writeDisposition: String,
    bqSink: BigQuerySink,
    schemaWithMergedMetadata: Schema
  ): BigQueryLoadConfig = {
    BigQueryLoadConfig(
      connectionRef = Some(mergedMetadata.getSinkConnectionRef()),
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
      session,
      errMessagesDS,
      domainName,
      schemaName,
      now
    ) match {
      case Success((rejectedDF, rejectedPath)) =>
        Success(rejectedPath)
      case Failure(exception) =>
        logger.error("Failed to save Rejected", exception)
        Failure(exception)
    }
  }
}
