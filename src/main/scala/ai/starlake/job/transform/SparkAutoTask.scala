package ai.starlake.job.transform

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.strategies.SparkSQLStrategiesBuilder
import ai.starlake.job.metrics.{ExpectationJob, SparkExpectationAssertionHandler}
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig, BigQuerySparkJob}
import ai.starlake.job.sink.es.{ESLoadConfig, ESLoadJob}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils._
import ai.starlake.utils.kafka.KafkaClient
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import better.files.File
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.PythonRunner
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}
import scala.util.{Failure, Success, Try}

class SparkAutoTask(
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

  override def run(): Try[JobResult] = {
    if (
      sinkConnection.getType() != ConnectionType.FS ||
      taskDesc.getDefaultConnection().getType() != ConnectionType.FS
    ) {
      runSparkOnAny()
    } else {
      runSparkOnSpark()
    }
    /*
    val res = runSpark()
    res match {
      case Success(SparkJobResult(None, _)) =>
      case Success(SparkJobResult(Some(dataframe), _)) if interactive.isEmpty =>
        sink(Some(dataframe))
      case Failure(_) =>
    }
    res
     */
  }

  def applyHiveTableAcl(forceApply: Boolean = false): Try[Unit] =
    Try {
      if (forceApply || settings.appConfig.accessPolicies.apply) {
        val sqls = extractHiveTableAcl()
        sqls.foreach { sql =>
          logger.info(sql)
          SparkUtils.sql(session, sql)
        }
      }
    }

  private def extractHiveTableAcl(): List[String] = {

    if (settings.appConfig.isHiveCompatible()) {
      taskDesc.acl.flatMap { ace =>
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

  val fullTableName = s"${taskDesc.domain}.${taskDesc.table}"

  private def sinkToFile2(dataframe: DataFrame, sink: FsSink): Boolean = {
    val coalesce = sink.coalesce.getOrElse(false)
    val targetPath = taskDesc.getTargetPath()
    logger.info(s"About to write resulting dataset to $targetPath")
    // Target Path exist only if a storage area has been defined at task or job level
    // To execute a task without writing to disk simply avoid the area at the job and task level

    val sinkPartition = sink.partition.getOrElse(Partition(attributes = taskDesc.partition))

    val partitionedDF =
      if (coalesce)
        dataframe.repartition(1)
      else
        dataframe

    val partitionedDFWriter = partitionedDF.write
    /*      partitionedDatasetWriter(
        partitionedDF,
        sinkPartition.attributes
      )
     */
    val clusteredDFWriter = sink.clustering match {
      case None          => partitionedDFWriter
      case Some(columns) => partitionedDFWriter.sortBy(columns.head, columns.tail: _*)
    }

    val finalDatasetWithoutPath = clusteredDFWriter
      .mode(taskDesc.getWrite().toSaveMode)
      .format(sink.format.getOrElse(settings.appConfig.defaultWriteFormat))
      .options(sink.getOptions())

    val finalDataset =
      if (settings.appConfig.privacyOnly) {
        finalDatasetWithoutPath.option("path", targetPath.toString)
      } else {
        finalDatasetWithoutPath
      }
    val fullTableName = s"${taskDesc.domain}.${taskDesc.table}"
    SparkUtils.createSchema(session, taskDesc.domain)
    SparkUtils.sql(session, s"USE ${taskDesc.domain}")
    if (taskDesc.getWrite().toSaveMode == SaveMode.Overwrite && tableExists)
      SparkUtils.sql(session, s"DROP TABLE IF EXISTS ${taskDesc.table}")
    finalDataset.saveAsTable(fullTableName)
    val tableTagPairs =
      Utils.extractTags(this.taskDesc.tags) + ("comment" -> taskDesc.comment.getOrElse(""))
    val tagsAsString = tableTagPairs.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
    SparkUtils.sql(session, s"ALTER TABLE $fullTableName SET TBLPROPERTIES($tagsAsString)")

    if (Utils.isRunningInDatabricks()) {
      taskDesc.attributesDesc.foreach { attrDesc =>
        SparkUtils.sql(
          session,
          s"ALTER TABLE ${taskDesc.table} CHANGE COLUMN $attrDesc.name COMMENT '${attrDesc.comment}'"
        )
      }
    }
    // TODO Handle SinkType.FS and SinkType to Hive in Sink section in the caller
    // We use the pathname when we write to FS
    if (coalesce) {
      val extension =
        sink.extension.getOrElse(
          sink.format.getOrElse(settings.appConfig.defaultWriteFormat)
        )
      finalDataset.option("path", targetPath.toString).save()
      val csvPath = storageHandler
        .list(targetPath, s".$extension", LocalDateTime.MIN, recursive = false)
        .head
      val finalPath = new Path(targetPath, targetPath.getName + s".$extension")
      storageHandler.move(csvPath, finalPath)
    }
    true
  }

  private def sinkToES(): Try[JobResult] = {
    val targetPath =
      new Path(DatasetArea.path(this.taskDesc.domain), this.taskDesc.table)
    val sink: EsSink = this.taskDesc.sink
      .map(_.getSink())
      .map(_.asInstanceOf[EsSink])
      .getOrElse(
        throw new Exception("Sink of type ES must be specified when loading data to ES !!!")
      )
    val esConfig =
      ESLoadConfig(
        timestamp = sink.timestamp,
        id = sink.id,
        format = settings.appConfig.defaultWriteFormat,
        domain = this.taskDesc.domain,
        schema = this.taskDesc.table,
        dataset = Some(Left(targetPath)),
        options = sink.getOptions()
      )

    val res = new ESLoadJob(esConfig, storageHandler, schemaHandler).run()
    res

  }

  def sink(maybeDataFrame: Option[DataFrame]): Boolean = {
    val sinkOption = this.sinkConfig
    logger.info(s"sinking data to $sinkOption")
    maybeDataFrame match {
      case None =>
        logger.info("No dataframe to sink. Sink done natively  to the source")
        true
      case Some(dataframe) =>
        val result =
          sinkOption match {
            case Some(sink) =>
              sink match {
                case _: EsSink =>
                  sinkToES()

                case _: FsSink =>
                  sinkToFile(dataframe)

                case _: BigQuerySink =>
                  sinkToBQ(dataframe)

                case _: JdbcSink =>
                  sinkToJDBC(dataframe)

                case _: KafkaSink =>
                  sinkToKafka(dataframe)
                case _ =>
                  dataframe.write.format("console").save()
                  throw new Exception(
                    s"No supported Sink is activated for this job $sink, dumping to console"
                  )

              }
            case None =>
              throw new Exception("Sink is not activated for this job")
          }
        Utils.throwFailure(result, logger)
    }
  }

  private def sinkToKafka(mergedDF: DataFrame): Try[DataFrame] = Try {
    Utils.withResources(new KafkaClient(settings.appConfig.kafka)) { kafkaClient =>
      kafkaClient.sinkToTopic(settings.appConfig.kafka.topics(taskDesc.table), mergedDF)
    }
    mergedDF
  }

  private def sinkToBQ2(dataframe: DataFrame): Try[JobResult] = {
    val bqSink =
      this.sinkConfig
        .getOrElse(
          throw new Exception("Sink of type BigQuery must be specified when loading data to BQ !!!")
        )
        .asInstanceOf[BigQuerySink]

    val source = Right(Utils.setNullableStateOfColumn(dataframe, nullable = true))
    val (createDisposition, writeDisposition) = {
      Utils.getDBDisposition(this.taskDesc.getWrite())
    }
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
        // outputTableDesc = action.taskDesc.comment.getOrElse(""),
        attributesDesc = this.taskDesc.attributesDesc,
        outputDatabase = this.taskDesc.database
      )
    val result =
      new BigQuerySparkJob(bqLoadConfig, None, this.taskDesc.comment).run()
    result
  }

  override def buildAllSQLQueries(sql: Option[String]): String = {
    assert(taskDesc.parseSQL.getOrElse(false))
    val columnNames = SQLUtils.extractColumnNames(sql.getOrElse(taskDesc.getSql()))
    val mainSql =
      new SparkSQLStrategiesBuilder()
        .buildSQLForStrategy(
          sql.getOrElse(taskDesc.getSql()),
          strategy,
          fullTableName,
          tableExists,
          columnNames,
          truncate,
          isMaterializedView(),
          jdbcSinkEngine
        )
    mainSql
  }

  def runSparkOnBigQuery(): Option[DataFrame] = {
    val config = BigQueryLoadConfig(
      connectionRef = Some(settings.appConfig.connectionRef)
    )
    val sqlWithParameters = substituteRefTaskMainSQL(taskDesc.getSql())
    val result = new BigQuerySparkJob(config).query(sqlWithParameters)
    result match {
      case Success(df) => Some(df)
      case Failure(e) =>
        logger.error("BigQuery query failed", e)
        throw e
    }
  }

  def runSparkOnJdbc(): Option[DataFrame] = {
    val runConnection = taskDesc.getDefaultConnection()
    val sqlWithParameters = substituteRefTaskMainSQL(taskDesc.getSql())
    val res = session.read
      .format(
        runConnection.sparkFormat.getOrElse(throw new Exception("Should never happen"))
      )
      .option("query", sqlWithParameters)
      .options(runConnection.options)
      .load()
    Some(res)
  }

  def runSparkOnAny(): Try[SparkJobResult] = Try {
    val dataFrameToSink = buildDataFrameToSink()
    if (interactive.isEmpty) {
      dataFrameToSink.map { df =>
        sink(Some(df))
      }
    }
    SparkJobResult(dataFrameToSink)
  }

  def runSparkOnFS(): Option[DataFrame] = {
    val sqlWithParameters = substituteRefTaskMainSQL(taskDesc.getSql())
    runSqls(List(sqlWithParameters), "Main")
  }

  private def buildDataFrameToSink(): Option[DataFrame] = {
    val dataframe = runEngine match {
      case Engine.SPARK =>
        runSparkOnFS()
      case Engine.BQ =>
        runSparkOnBigQuery()
      case Engine.JDBC =>
        runSparkOnJdbc()
      case _ =>
        throw new Exception(s"Unsupported engine ${runEngine}")
    }
    dataframe
  }

  def runSparkOnSpark(): Try[SparkJobResult] = {
    val start = Timestamp.from(Instant.now())
    Try {

      // we replace any ref in the sql
      val sqlNoRefs = substituteRefTaskMainSQL(taskDesc.getSql())
      val jobResult = interactive match {
        case Some(_) =>
          // just run the request and return the dataframe
          val df = SparkUtils.sql(session, sqlNoRefs)
          SparkJobResult(Some(df))
        case None =>
          runSqls(preSql, "Pre")
          val jobResult =
            (taskDesc.sql, taskDesc.python) match {
              case (Some(_), None) =>
                val sqlToRun =
                  if (taskDesc.parseSQL.getOrElse(true)) {
                    // we need to generate the insert / merge / create table
                    buildAllSQLQueries(Some(sqlNoRefs))
                  } else {
                    // we just run the sql since ethe user has provided the sql to run
                    sqlNoRefs
                  }
                runSqls(sqlToRun.splitSql(), "Main")
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
          runSqls(postSql, "Post")
          if (settings.appConfig.expectations.active) {
            new ExpectationJob(
              taskDesc.database,
              taskDesc.domain,
              taskDesc.table,
              taskDesc.expectations,
              storageHandler,
              schemaHandler,
              new SparkExpectationAssertionHandler(session)
            ).run()
          }
          applyHiveTableAcl()
          SparkJobResult(jobResult)
      }
      val end = Timestamp.from(Instant.now())
      logAuditSuccess(start, end, -1)
      jobResult
    } recoverWith { case e: Exception =>
      val end = Timestamp.from(Instant.now())
      logAuditFailure(start, end, e)
      Failure(e)
    }
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

  def runSqls(sqls: List[String], typ: String): Option[DataFrame] = {
    if (sqls.nonEmpty) {
      logger.info(s"Running Spark $typ SQL")
      val df = sqls.map { sql =>
        SparkUtils.sql(session, sql)
      }.lastOption
      df
    } else {
      None
    }
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

    Try(SparkUtils.sql(session, script)).isSuccess

  }

  override lazy val tableExists: Boolean = {
    val exists = session.catalog.tableExists(taskDesc.domain, taskDesc.table)
    if (!exists && taskDesc._auditTableName.isDefined)
      createAuditTable() // We are sinking to an audit table. We need to create it first
    exists
  }

  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////
  //////////// SPARK SINK ///////////////////////////
  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////
  private def updateSparkTableSchema(incomingSchema: StructType): Unit = {
    val incomingSchemaWithSCD2Support =
      if (strategy.`type` == StrategyType.SCD2) {
        val res =
          incomingSchema
            .add(
              StructField(
                strategy.start_ts
                  .getOrElse(settings.appConfig.scd2StartTimestamp),
                TimestampType,
                nullable = true
              )
            )
            .add(
              StructField(
                strategy.end_ts.getOrElse(settings.appConfig.scd2EndTimestamp),
                TimestampType,
                nullable = true
              )
            )
        res
      } else {
        incomingSchema
      }

    if (tableExists) {
      // Load from accepted area
      // We provide the accepted DF schema since partition columns types are inferred when parquet is loaded and might not match with the DF being ingested
      val existingTableSchema = session.table(fullTableName).schema
      val newFields =
        MergeUtils.computeNewColumns(existingTableSchema, incomingSchemaWithSCD2Support)
      if (newFields.nonEmpty) {
        val colsAsString = newFields
          .map(field =>
            s"""${field.name} ${field.dataType.sql} comment "${field
                .getComment()
                .getOrElse("")}" """
          )
          .mkString(",")
        SparkUtils.sql(session, s"ALTER TABLE $fullTableName ADD columns ($colsAsString)")
      }
      Some(StructType(existingTableSchema.fields ++ newFields))
    } else {
      val sink =
        sinkConfig
          .getOrElse(throw new Exception("Should never happen"))
          .asInstanceOf[FsSink]
      val partitionedByClause =
        sink.partition.map(_.attributes).map(_.mkString("PARTITIONED BY (", ",", ")")) getOrElse ""

      val clusterByClause =
        sink.clustering.map(_.mkString("CLUSTERED BY (", ",", ")")) getOrElse ""

      val options =
        sink.options
          .map(_.map { case (k, v) => s"'$k'='$v'" }
            .mkString("OPTIONS(", ",", ")"))
          .getOrElse("")

      val comment = taskDesc.comment.map(c => s"COMMENT '$c'").getOrElse("")
      val tableTagPairs = Utils.extractTags(taskDesc.tags)
      val tblProperties = {
        if (tableTagPairs.isEmpty) ""
        else
          tableTagPairs.map { case (k, v) => s"'$k'='$v'" }.mkString("TBLPROPERTIES(", ",", ")")
      }

      val fields = incomingSchemaWithSCD2Support.fields
        .map(field =>
          s"""${field.name} ${field.dataType.sql} comment "${field.getComment().getOrElse("")}" """
        )
        .mkString(",")

      SparkUtils.createSchema(session, taskDesc.domain)

      val ddlTable =
        s"""CREATE TABLE $fullTableName($fields)
           |USING ${settings.appConfig.defaultWriteFormat}
           |$options
           |$partitionedByClause
           |$clusterByClause
           |$comment
           |$tblProperties
           |""".stripMargin
      session.sql(ddlTable)
    }
  }

  private def sinkToFile(
    dataset: DataFrame
  ): Try[JobResult] = {
    // Ingestion done with Spark but not yet sinked.
    // This is called by sinkRejected and sinkAccepted
    val writeMode = strategy.`type`.toWriteMode()
    dataset.createOrReplaceTempView("SL_INTERNAL_VIEW")
    val incomingSchema = dataset.schema
    val allAttributes = incomingSchema.fieldNames.mkString(",")
    // We check if the table exists before updating the table schema below
    if (taskDesc._dbComment.nonEmpty || taskDesc.tags.nonEmpty) {
      val domainComment = taskDesc._dbComment.getOrElse("")
      val tableTagPairs = Utils.extractTags(taskDesc.tags) + ("comment" -> domainComment)
      val tagsAsString = tableTagPairs.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
      SparkUtils.sql(
        session,
        s"CREATE SCHEMA IF NOT EXISTS ${taskDesc.domain} WITH DBPROPERTIES($tagsAsString)"
      )
    } else {
      SparkUtils.createSchema(session, taskDesc.domain)
    }
    if (taskDesc._auditTableName.isEmpty) {
      // We are not writing to an audit table. We are writing to the final table
      // Update the table schema and create it if required
      updateSparkTableSchema(incomingSchema)
    }

    val sqls =
      new SparkSQLStrategiesBuilder().buildSQLForStrategy(
        s"SELECT $allAttributes FROM SL_INTERNAL_VIEW",
        strategy,
        fullTableName,
        tableExists,
        incomingSchema.fieldNames.toList,
        truncate = truncate,
        isMaterializedView(),
        settings.appConfig.jdbcEngines("spark")
      )

    if (dataset.columns.length > 0) {
      val result = Try {
        val df = runSqls(sqls.splitSql(), "Main")
        if (taskDesc._auditTableName.isEmpty) {
          if (taskDesc._dbComment.nonEmpty || taskDesc.tags.nonEmpty) {
            val tableComment = taskDesc.comment.getOrElse("")
            val tableTagPairs = Utils.extractTags(taskDesc.tags) + ("comment" -> tableComment)
            val tagsAsString = tableTagPairs.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
            SparkUtils.sql(session, s"ALTER TABLE $fullTableName SET TBLPROPERTIES($tagsAsString)")
          }
          applyHiveTableAcl()
        }
        SparkJobResult(df)
      }
      result
    } else {
      Success(SparkJobResult(None))
    }
  }

  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////
  //////////// BQ SINK //////////////////////////////
  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////

  private def sinkToBQ(loadedDF: DataFrame): Try[JobResult] = {
    val twoSteps = strategy.isMerge()
    if (twoSteps) {
      val (overwriteCreateDisposition: String, overwriteWriteDisposition: String) =
        Utils.getDBDisposition(WriteMode.OVERWRITE)

      val bqTableSchema = Some(BigQuerySchemaConverters.toBigQuerySchema(loadedDF.schema))
      val tempTablePartName = SQLUtils.temporaryTableName(taskDesc.table)
      val firstStepTemplateTableId =
        BigQueryJobBase.extractProjectDatasetAndTable(
          taskDesc.database,
          taskDesc.domain,
          tempTablePartName
        )

      val config = BigQueryLoadConfig(
        connectionRef = Some(sinkConnectionRef),
        source = Right(loadedDF),
        outputTableId = Some(firstStepTemplateTableId),
        sourceFormat = settings.appConfig.defaultWriteFormat,
        createDisposition = overwriteCreateDisposition,
        writeDisposition = overwriteWriteDisposition,
        days = Some(1),
        outputDatabase = taskDesc.database
      )

      val sparkBigQueryJob = new BigQuerySparkJob(config, bqTableSchema, None)
      val firstStepJobResult = sparkBigQueryJob.run()

      firstStepJobResult match {
        case Success(_) =>
          val allAttributeNames = loadedDF.schema.fields.map(_.name)
          val attributesSelectAsString = allAttributeNames.mkString(",")

          val secondStepTask = new BigQueryAutoTask(
            taskDesc.copy(
              name = fullTableName,
              sql = Some(
                s"SELECT $attributesSelectAsString FROM ${taskDesc.domain}.$tempTablePartName"
              )
            ),
            commandParameters,
            interactive,
            truncate,
            resultPageSize
          )
          secondStepTask.updateBigQueryTableSchema(loadedDF.schema)
          val secondStepJobResult = secondStepTask.run()
          secondStepJobResult
        case Failure(e) =>
          Failure(e)
      }
    } else {
      val secondStepDesc = taskDesc.copy(
        name = fullTableName,
        sql = None
      )
      // Update table schema
      val secondSTepTask =
        new BigQueryAutoTask(
          secondStepDesc,
          commandParameters,
          interactive,
          truncate,
          resultPageSize
        )
      secondSTepTask.updateBigQueryTableSchema(loadedDF.schema)
      secondSTepTask.runOnDF(loadedDF)

    }
  }

  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////
  //////////// JDBC SINK ///////////////////////////
  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////
  def sinkToJDBC(loadedDF: DataFrame): Try[JobResult] = {
    val targetTableName = s"${taskDesc.domain}.${taskDesc.table}"
    val sinkConnectionRefOptions = sinkConnection.options

    val allAttributeNames = loadedDF.schema.fields.map(_.name)
    val attributesSelectAsString = allAttributeNames.mkString(",")

    val targetTableExists: Boolean = {
      JdbcDbUtils.withJDBCConnection(sinkConnectionRefOptions) { conn =>
        val url = sinkConnection.options("url")
        JdbcDbUtils.tableExists(conn, url, targetTableName)
      }
    }

    val twoSteps = strategy.isMerge()
    val result =
      if (twoSteps) {
        val tablePartName = SQLUtils.temporaryTableName(taskDesc.table)
        val firstStepTempTable = s"${taskDesc.domain}.$tablePartName"

        if (settings.appConfig.createSchemaIfNotExists) {
          JdbcDbUtils.withJDBCConnection(sinkConnectionRefOptions) { conn =>
            JdbcDbUtils.createSchema(conn, taskDesc.domain)
          }
        }
        val jdbcUrl = sinkConnectionRefOptions("url")
        JdbcDbUtils.withJDBCConnection(sinkConnectionRefOptions) { conn =>
          SparkUtils.createTable(
            conn,
            firstStepTempTable,
            loadedDF.schema,
            caseSensitive = false,
            new JdbcOptionsInWrite(jdbcUrl, firstStepTempTable, sinkConnectionRefOptions)
          )
        }
        loadedDF.write
          .format("jdbc")
          .option("dbtable", firstStepTempTable)
          .mode(SaveMode.Append) // Because Overwrite loose the schema and require us to add quotes
          .options(sinkConnectionRefOptions)
          .save()

        logger.info(
          s"JDBC save done to table ${firstStepTempTable}"
        )

        // We now have a table in the database.
        // We can now run the merge statement using the native SQL capabilities
        val secondStepTaskDesc =
          taskDesc.copy(
            name = fullTableName,
            sql = Some(s"SELECT $attributesSelectAsString FROM $firstStepTempTable")
          )

        val secondStepAutoTask =
          new JdbcAutoTask(secondStepTaskDesc, Map.empty, None, truncate = false)(
            settings,
            storageHandler,
            schemaHandler
          )
        secondStepAutoTask.updateJdbcTableSchema(loadedDF.schema, fullTableName)
        val jobResult = secondStepAutoTask.runJDBC(None)

        JdbcDbUtils.withJDBCConnection(sinkConnectionRefOptions) { conn =>
          JdbcDbUtils.dropTable(firstStepTempTable, conn)
        }
        jobResult
      } else {
        // We have a OVERWRITE / APPEND strategy, we can write directly to the direct table using Spark.
        // No need to pass by an intermediary table

        val secondStepDesc = taskDesc.copy(
          name = fullTableName,
          sql = None
        )
        val secondAutoStepTask =
          new JdbcAutoTask(secondStepDesc, Map.empty, None, truncate = false)
        secondAutoStepTask.updateJdbcTableSchema(loadedDF.schema, fullTableName)
        val jobResult = secondAutoStepTask.runJDBC(Some(loadedDF))
        jobResult
      }
    result
  }

}
