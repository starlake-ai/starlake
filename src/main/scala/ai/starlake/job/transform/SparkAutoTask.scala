package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.extract.{JdbcDbUtils, SparkExtractorJob}
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
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.PythonRunner
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.Timestamp
import java.time.Instant
import scala.util.{Failure, Success, Try}

class SparkAutoTask(
  appId: Option[String],
  taskDesc: AutoTaskDesc,
  commandParameters: Map[String, String],
  interactive: Option[String],
  truncate: Boolean,
  test: Boolean,
  logExecution: Boolean,
  accessToken: Option[String] = None,
  resultPageSize: Int = 1,
  schema: Option[Schema] = None
)(implicit settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends AutoTask(
      appId,
      taskDesc,
      commandParameters,
      interactive,
      test,
      logExecution,
      truncate,
      resultPageSize
    ) {

  override def run(): Try[JobResult] = {
    val result =
      interactive match {
        case Some(_) =>
          runSparkOnSpark(taskDesc.getSql())
        case None =>
          (taskDesc.getRunConnection().`type`, sinkConnection.`type`) match {
            case (
                  ConnectionType.FS,
                  ConnectionType.FS
                ) => // databricks to databricks including fs (text, csv ...)
              if (sinkConfig.asInstanceOf[FsSink].isExport()) {
                runSparkOnAny() // We exporting from Spark to the filesystem
              } else {
                runSparkOnSpark(taskDesc.getSql())
              }

            case _ =>
              runSparkOnAny()
          }
      }
    result
  }

  def applyHiveTableAcl(forceApply: Boolean = false): Try[Unit] =
    Try {
      val isGrantSupported = Try(
        session.sessionState.sqlParser.parseExpression(
          "grant select on table secured_table to role my_role"
        )
      ).isSuccess
      if (isGrantSupported) {
        if (forceApply || settings.appConfig.accessPolicies.apply) {
          val sqls = extractHiveTableAcl()
          sqls.foreach { sql =>
            logger.info(sql)
            SparkUtils.sql(session, sql)
          }
        }
      } else {
        logger.warn(
          "GRANT are not supported in this version of Spark."
        )
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

  private def sinkToES(dataframe: DataFrame): Try[JobResult] = {
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
        domain = taskDesc.domain,
        schema = taskDesc.table,
        dataset = Some(Right(dataframe)),
        options = sink.getOptions()
      )

    val res = new ESLoadJob(esConfig, storageHandler, schemaHandler).run()
    res

  }

  def sink(dataframe: DataFrame): Boolean = {
    val sink = this.sinkConfig
    logger.info(s"sinking data to $sink")
    val result =
      sink match {
        case _: EsSink =>
          sinkToES(dataframe)

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
    Utils.throwFailure(result, logger)
  }

  private def sinkToKafka(mergedDF: DataFrame): Try[DataFrame] = Try {
    Utils.withResources(new KafkaClient(settings.appConfig.kafka)) { kafkaClient =>
      kafkaClient.sinkToTopic(settings.appConfig.kafka.topics(taskDesc.table), mergedDF)
    }
    mergedDF
  }

  def buildDataframeFromBigQuery(): Option[DataFrame] = {
    val config = BigQueryLoadConfig(
      connectionRef = Some(this.taskDesc.getRunConnectionRef()),
      accessToken = accessToken
    )
    val sqlWithParameters =
      schemaHandler.substituteRefTaskMainSQL(taskDesc.getSql(), taskDesc.getRunConnection())
    val result = new BigQuerySparkJob(config).query(sqlWithParameters)
    result match {
      case Success(df) => Some(df)
      case Failure(e) =>
        logger.error("BigQuery query failed", e)
        throw e
    }
  }

  def buildDataframeFromJdbc(): Option[DataFrame] = {
    val runConnection = taskDesc.getRunConnection()
    val sqlWithParameters =
      schemaHandler.substituteRefTaskMainSQL(taskDesc.getSql(), taskDesc.getRunConnection())
    val res = session.read
      .format(
        runConnection.sparkDatasource().getOrElse("jdbc")
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
        sink(df)
      }
    }
    SparkJobResult(dataFrameToSink, None)
  }

  def buildDataframeFromFS(): Option[DataFrame] = {
    val sqlWithParameters =
      schemaHandler.substituteRefTaskMainSQL(taskDesc.getSql(), taskDesc.getRunConnection())
    runSqls(List(sqlWithParameters), "Main")
  }

  private def buildDataFrameToSink(): Option[DataFrame] = {
    val runConnectionType = taskDesc.getRunConnectionType()
    val runEngine: Engine = taskDesc.getRunEngine()

    val dataframe =
      (runEngine, runConnectionType) match {
        case (Engine.SPARK, ConnectionType.FS) =>
          buildDataframeFromFS()
        case (Engine.SPARK, ConnectionType.BQ) =>
          buildDataframeFromBigQuery()
        case (Engine.SPARK, ConnectionType.JDBC) =>
          buildDataframeFromJdbc()
        case (Engine.BQ, ConnectionType.BQ) =>
          buildDataframeFromBigQuery()
        case (Engine.JDBC, ConnectionType.JDBC) =>
          buildDataframeFromJdbc()
        case _ =>
          throw new Exception(
            s"Unsupported engine $runEngine and connection type $runConnectionType"
          )
      }
    dataframe
  }

  def runSparkOnSpark(sql: String): Try[SparkJobResult] = {
    val start = Timestamp.from(Instant.now())
    Try {
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

      // we replace any ref in the sql
      val sqlNoRefs = schemaHandler.substituteRefTaskMainSQL(sql, taskDesc.getRunConnection())
      val jobResult = interactive match {
        case Some(_) =>
          // just run the request and return the dataframe
          val df =
            SparkUtils.sql(session, sqlNoRefs).limit(settings.appConfig.maxInteractiveRecords)
          SparkJobResult(Some(df), None)
        case None =>
          runSqls(preSql, "Pre")
          val jobResult =
            (sql, taskDesc.python) match {
              case (_, None) =>
                val sqlToRun =
                  if (taskDesc.parseSQL.getOrElse(true)) {
                    // we need to generate the insert / merge / create table except when exporting to csv & xls
                    buildAllSQLQueries(Some(sqlNoRefs))
                  } else {
                    // we just run the sql since ethe user has provided the sql to run
                    sqlNoRefs
                  }
                val result = runSqls(sqlToRun.splitSql(), "Main")
                result

              case ("", Some(pythonFile)) =>
                runPySpark(pythonFile)
              case (_, _) =>
                throw new Exception(
                  s"Only one of 'sql' or 'python' attribute may be defined ${taskDesc.name}"
                )
            }
          runSqls(postSql, "Post")
          if (taskDesc._auditTableName.isEmpty) {
            if (taskDesc.comment.nonEmpty || taskDesc.tags.nonEmpty) {
              val tableComment = taskDesc.comment.getOrElse("")
              val tableTagPairs = Utils.extractTags(taskDesc.tags) + ("comment" -> tableComment)
              val tagsAsString = tableTagPairs.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
              SparkUtils.sql(
                session,
                s"ALTER TABLE $fullTableName SET TBLPROPERTIES($tagsAsString)"
              )
              if (settings.appConfig.autoExportSchema) {
                val domains = new SparkExtractorJob(Map(taskDesc.domain -> List(taskDesc.table)))
                  .schemasAndTables()
                domains match {
                  case Success(domains) =>
                    schemaHandler.saveToExternals(domains)
                  case Failure(e) =>
                    logger.error(s"Failed to extract domain", e)
                }
              }
            }
            applyHiveTableAcl()
          }
          if (settings.appConfig.expectations.active) {
            new ExpectationJob(
              Option(applicationId()),
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
          SparkJobResult(jobResult, None)
      }
      val end = Timestamp.from(Instant.now())
      if (logExecution)
        logAuditSuccess(start, end, -1, test)
      jobResult
    } recoverWith { case e: Exception =>
      val end = Timestamp.from(Instant.now())
      logAuditFailure(start, end, e, test)
      Failure(e)
    }
  }

  private def runPySpark(pythonFile: Path): Option[DataFrame] = {
    // We first download locally all files because PythonRunner only support local filesystem
    val pyFiles =
      pythonFile +: settings.sparkConfig
        .getString("pyFiles")
        .split(",")
        .filter(_.nonEmpty)
        .map(x => new Path(x.trim))
    val directory = new Path(File.newTemporaryDirectory().pathAsString)
    logger.info(s"Python local directory is $directory")
    pyFiles.foreach { pyFile =>
      val pyName = pyFile.getName
      storageHandler.copyToLocal(pyFile, new Path(directory, pyName))
    }
    val pythonParams = commandParameters.flatMap { case (name, value) =>
      List(s"""--$name""", s"""$value""")
    }.toArray

    PythonRunner.main(
      Array(
        new Path(directory, pythonFile.getName).toString,
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
      val df = sqls.map { sql =>
        SparkUtils.sql(session, sql)
      }.lastOption
      df
    } else {
      None
    }
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

    SparkUtils.createSchema(session, fullDomainName)
    Try(SparkUtils.sql(session, script)).isSuccess

  }

  override def tableExists: Boolean = {
    val sink = this.sinkConfig
    val result = {
      Try {
        sink match {
          case _: FsSink =>
            val exists = session.catalog.tableExists(taskDesc.domain, taskDesc.table)
            if (!exists && taskDesc._auditTableName.isDefined) {
              createAuditTable()
            } else
              exists

          case _: BigQuerySink =>
            val bqJob =
              new BigQueryAutoTask(
                Option(applicationId()),
                this.taskDesc,
                Map.empty,
                None,
                truncate = false,
                test,
                logExecution
              )(
                settings,
                storageHandler,
                schemaHandler
              )
            bqJob.tableExists
          case _: JdbcSink =>
            val jdbcJob =
              new JdbcAutoTask(
                Option(applicationId()),
                this.taskDesc,
                Map.empty,
                None,
                truncate = false,
                test,
                logExecution = logExecution
              )(
                settings,
                storageHandler,
                schemaHandler
              )
            jdbcJob.tableExists
          case other =>
            throw new Exception(
              s"No supported on $other"
            )

        }
      }
    }
    Utils.throwFailure(result, logger)
    logger.info(s"tableExists $fullTableName: $result")
    result.getOrElse(false)
  }

  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////
  //////////// SPARK SINK ///////////////////////////
  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////
  private def updateSparkTableSchema(incomingSchema: StructType): Unit = {
    val incomingSchemaWithSCD2Support =
      if (strategy.getEffectiveType() == WriteStrategyType.SCD2) {
        val startTs = strategy.startTs.getOrElse(settings.appConfig.scd2StartTimestamp)
        val endTs = strategy.endTs.getOrElse(settings.appConfig.scd2EndTimestamp)

        val scd2FieldsFound =
          incomingSchema.fields.exists(_.name.toLowerCase() == startTs.toLowerCase())

        if (!scd2FieldsFound) {
          val incomingSchemaWithScd2 =
            incomingSchema
              .add(
                StructField(
                  startTs,
                  TimestampType,
                  nullable = true
                )
              )
              .add(
                StructField(
                  endTs,
                  TimestampType,
                  nullable = true
                )
              )
          incomingSchemaWithScd2
        } else {
          incomingSchema
        }

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

        SparkUtils.sql(session, s"ALTER TABLE $fullTableName ADD COLUMNS ($colsAsString)")
      }
    } else {
      val sink =
        sinkConfig
          .asInstanceOf[FsSink]

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
           |USING ${sink.getStorageFormat()}
           |${sink.getTableOptionsClause()}
           |${sink.getPartitionByClauseSQL()}
           |${sink.getClusterByClauseSQL()}
           |$comment
           |$tblProperties
           |""".stripMargin
      logger.info(s"Creating table $fullTableName with DDL $ddlTable")
      session.sql(ddlTable)
    }
  }

  protected def effectiveSinkToFile(dataset: DataFrame): Try[JobResult] = {
    val allAttributes = dataset.schema.fieldNames.mkString(",")
    dataset.createOrReplaceTempView("SL_INTERNAL_VIEW")
    runSparkOnSpark(s"SELECT $allAttributes FROM SL_INTERNAL_VIEW")
  }

  private def sinkToFile(
    dataset: DataFrame
  ): Try[JobResult] = {
    // Ingestion done with Spark but not yet sinked.
    // This is called by sinkRejected and sinkAccepted
    // We check if the table exists before updating the table schema below
    val incomingSchema = dataset.schema
    val fsSink = sinkConfig.asInstanceOf[FsSink]
    if (taskDesc._auditTableName.isEmpty && !fsSink.isExport()) {
      // We are not writing to an audit table. We are writing to the final table
      // Update the table schema and create it if required
      updateSparkTableSchema(incomingSchema)
    }

    val hasColumns: Boolean = dataset.columns.length > 0
    if (!hasColumns) {
      Success(SparkJobResult(None, None))
    } else {
      effectiveSinkToFile(dataset)
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
        outputDatabase = taskDesc.database,
        accessToken = accessToken
      )

      val sparkBigQueryJob = new BigQuerySparkJob(config, bqTableSchema, None)
      val firstStepJobResult = sparkBigQueryJob.run()

      firstStepJobResult match {
        case Success(_) =>
          val allAttributeNames = loadedDF.schema.fields.map(_.name)
          val attributesSelectAsString = allAttributeNames.mkString(",")

          val secondStepTask = new BigQueryAutoTask(
            Option(applicationId()),
            taskDesc.copy(
              name = fullTableName,
              sql = Some(
                s"SELECT $attributesSelectAsString FROM ${taskDesc.domain}.$tempTablePartName"
              )
            ),
            commandParameters,
            interactive,
            truncate,
            test,
            logExecution,
            accessToken,
            resultPageSize
          )
          secondStepTask.updateBigQueryTableSchema(loadedDF.schema)
          val secondStepJobResult = secondStepTask.run()
          sparkBigQueryJob.dropTable(firstStepTemplateTableId)
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
          Option(applicationId()),
          secondStepDesc,
          commandParameters,
          interactive,
          truncate,
          test,
          logExecution,
          accessToken,
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

    /*
    val targetTableExists: Boolean = { // TODO declaration is never used
      JdbcDbUtils.withJDBCConnection(sinkConnectionRefOptions) { conn =>
        val url = sinkConnection.options("url")
        JdbcDbUtils.tableExists(conn, url, targetTableName)
      }
    }
     */

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
            new JdbcOptionsInWrite(jdbcUrl, firstStepTempTable, sinkConnectionRefOptions),
            attDdl()
          )
        }

        val tablePath = new Path(s"${settings.appConfig.datasets}/${fullTableName}")

        if (settings.storageHandler().exists(tablePath)) {
          settings.storageHandler().delete(tablePath)
        }

        if (sinkConnection.isDuckDb()) {
          loadedDF.write
            .format("parquet")
            .mode(SaveMode.Overwrite) // truncate done above if requested
            .save(tablePath.toString)
          JdbcDbUtils.withJDBCConnection(
            sinkConnection.options.updated("enable_external_access", "true")
          ) { conn =>
            val sql =
              s"INSERT INTO $fullTableName SELECT * FROM '$tablePath/*.parquet'"
            JdbcDbUtils.executeUpdate(sql, conn)
          }
          settings.storageHandler().delete(tablePath)
        } else {
          loadedDF.write
            .format(sinkConnection.sparkDatasource().getOrElse("jdbc"))
            .option("dbtable", fullTableName)
            .mode(SaveMode.Append) // truncate done above if requested
            .options(sinkConnection.options)
            .save()
        }

        logger.info(
          s"JDBC save done to table $firstStepTempTable"
        )

        // We now have a table in the database.
        // We can now run the merge statement using the native SQL capabilities
        val secondStepTaskDesc =
          taskDesc.copy(
            name = fullTableName,
            sql = Some(s"SELECT $attributesSelectAsString FROM $firstStepTempTable")
          )

        val secondStepAutoTask =
          new JdbcAutoTask(
            Option(applicationId()),
            secondStepTaskDesc,
            Map.empty,
            None,
            truncate = false,
            test,
            logExecution = logExecution
          )(
            settings,
            storageHandler,
            schemaHandler
          )
        secondStepAutoTask.updateJdbcTableSchema(loadedDF.schema, fullTableName)
        val jobResult = secondStepAutoTask.runJDBC(None)

        JdbcDbUtils.withJDBCConnection(sinkConnectionRefOptions) { conn =>
          JdbcDbUtils.dropTable(conn, firstStepTempTable)
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
          new JdbcAutoTask(
            Option(applicationId()),
            secondStepDesc,
            Map.empty,
            None,
            truncate = false,
            test,
            logExecution = logExecution
          )
        secondAutoStepTask.updateJdbcTableSchema(loadedDF.schema, fullTableName)
        val jobResult = secondAutoStepTask.runJDBC(Some(loadedDF))
        jobResult
      }
    result
  }
}

object SparkAutoTask extends StrictLogging {
  def executeUpdate(
    sql: String,
    connectionName: String
  )(implicit iSettings: Settings): Try[Boolean] = {
    val job = new SparkJob {
      override def name: String = "BigQueryTablesInfo"

      override implicit def settings: Settings = iSettings

      /** Just to force any job to implement its entry point using within the "run" method
        *
        * @return
        *   : Spark Dataframe for Spark Jobs None otherwise
        */
      override def run(): Try[JobResult] = Try {
        val df = session.sql(sql)
        SparkJobResult(Option(df), None)
      }
    }

    val jobResult = job.run()
    jobResult match {
      case Success(_) =>
        Success(true)
      case Failure(e) =>
        logger.error("Error while executing SQL", e)
        Failure(e)
    }
  }
}
