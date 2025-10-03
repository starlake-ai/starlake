package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
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
import java.time.Instant
import scala.util.{Failure, Success, Try}

class SparkAutoTask(
  taskDesc: AutoTaskDesc,
  commandParameters: Map[String, String],
  interactive: Option[String],
  truncate: Boolean,
  test: Boolean,
  resultPageSize: Int = 1
)(implicit settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends AutoTask(
      taskDesc,
      commandParameters,
      interactive,
      test,
      truncate,
      resultPageSize
    ) {

  override def run(): Try[JobResult] = {
    val result =
      (taskDesc.getRunConnection().getType(), sinkConnection.getType()) match {
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
    result
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

  def sink(dataframe: DataFrame, slSchema: Option[Schema] = None): Boolean = {
    val sink = this.sinkConfig
    logger.info(s"sinking data to $sink")
    val result =
      sink match {
        case _: EsSink =>
          sinkToES(dataframe)

        case _: FsSink =>
          sinkToFile(dataframe)

        case _: BigQuerySink =>
          sinkToBQ(dataframe, slSchema)

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

  private def sinkToBQ2(dataframe: DataFrame): Try[JobResult] = { // TODO declaration is never used
    val bqSink = this.sinkConfig.asInstanceOf[BigQuerySink]

    val source = Right(Utils.setNullableStateOfColumn(dataframe, nullable = true))
    val (createDisposition, writeDisposition) = {
      Utils.getDBDisposition(this.taskDesc.getWriteMode())
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
        outputPartition = bqSink.getPartitionColumn(),
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

  /*override def buildAllSQLQueries(sql: Option[String]): String = {
    assert(taskDesc.parseSQL.getOrElse(true))
    val columnNames = SQLUtils.extractColumnNames(sql.getOrElse(taskDesc.getSql()))
    val mainSql =
      StrategiesBuilder(jdbcSinkEngine.strategyBuilder)
        .buildSQLForStrategy(
          strategy,
          sql.getOrElse(taskDesc.getSql()),
          fullTableName,
          columnNames,
          tableExists,
          truncate,
          isMaterializedView(),
          jdbcSinkEngine,
          sinkConfig
        )
    mainSql
  }*/

  def runSparkQueryOnBigQuery(): Option[DataFrame] = {
    val config = BigQueryLoadConfig(
      connectionRef = Some(this.taskDesc.getRunConnectionRef())
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

  def runSparkQueryOnJdbc(): Option[DataFrame] = {
    val runConnection = taskDesc.getRunConnection()
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
        sink(df)
      }
    }
    SparkJobResult(dataFrameToSink)
  }

  def runSparkQueryOnFS(): Option[DataFrame] = {
    val sqlWithParameters = substituteRefTaskMainSQL(taskDesc.getSql())
    runSqls(List(sqlWithParameters), "Main")
  }

  private def buildDataFrameToSink(): Option[DataFrame] = {
    val runConnectionType = taskDesc.getRunConnectionType()
    val dataframe =
      (runEngine, runConnectionType) match {
        case (Engine.SPARK, ConnectionType.FS) =>
          runSparkQueryOnFS()
        case (Engine.SPARK, ConnectionType.BQ) =>
          runSparkQueryOnBigQuery()
        case (Engine.SPARK, ConnectionType.JDBC) =>
          runSparkQueryOnJdbc()
        case (Engine.BQ, ConnectionType.BQ) =>
          runSparkQueryOnBigQuery()
        case (Engine.JDBC, ConnectionType.JDBC) =>
          runSparkQueryOnJdbc()
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
      val sqlNoRefs = substituteRefTaskMainSQL(sql)
      val jobResult = interactive match {
        case Some(_) =>
          // just run the request and return the dataframe
          val df = SparkUtils.sql(session, sqlNoRefs)
          SparkJobResult(Some(df))
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
            }
            /////////////////

            /////////////////
            applyHiveTableAcl()
          }
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
      logAuditSuccess(start, end, -1, test = test)
      jobResult
    } recoverWith { case e: Exception =>
      val end = Timestamp.from(Instant.now())
      logAuditFailure(start, end, e, test = test)
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
              new BigQueryAutoTask(this.taskDesc, Map.empty, None, truncate = false, test = test)(
                settings,
                storageHandler,
                schemaHandler
              )
            bqJob.tableExists
          case _: JdbcSink =>
            val jdbcJob =
              new JdbcAutoTask(this.taskDesc, Map.empty, None, truncate = false, test = test)(
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
      if (strategy.getStrategyType() == WriteStrategyType.SCD2) {
        val startTs = strategy.start_ts.getOrElse(settings.appConfig.scd2StartTimestamp)
        val endTs = strategy.end_ts.getOrElse(settings.appConfig.scd2EndTimestamp)

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
        SparkUtils.sql(session, s"ALTER TABLE $fullTableName ADD columns ($colsAsString)")
      }
      Some(StructType(existingTableSchema.fields ++ newFields))
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
      Success(SparkJobResult(None))
    } else {
      effectiveSinkToFile(dataset)
    }
  }

  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////
  //////////// BQ SINK //////////////////////////////
  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////

  private def sinkToBQ(loadedDF: DataFrame, slSchema: Option[Schema] = None): Try[JobResult] = {
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
            test,
            resultPageSize
          )
          val secondStepJobResult = secondStepTask.runNative(loadedDF.schema)
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
      val secondStepTask =
        new BigQueryAutoTask(
          secondStepDesc,
          commandParameters,
          interactive,
          truncate,
          test,
          resultPageSize
        )
      val sparkSchema = loadedDF.schema
      val updateSchema = slSchema
        .map { slSchema =>
          val fields =
            sparkSchema.fields.map { field =>
              val slField = slSchema.attributes.find { attr =>
                attr.getFinalName().equalsIgnoreCase(field.name) &&
                attr.`type`.toLowerCase() == "variant"
              }
              slField
                .map { _ =>
                  val metadata =
                    org.apache.spark.sql.types.Metadata.fromJson("""{ "sqlType" : "JSON"}""")
                  val jsonField = field.copy(metadata = metadata)
                  jsonField
                }
                .getOrElse(field)
            }
          val updatedSchema = StructType(fields)
          updatedSchema
        }
        .getOrElse(sparkSchema)
      secondStepTask.runOnDF(loadedDF, Some(updateSchema))

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

    val targetTableExists: Boolean = { // TODO declaration is never used
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
            new JdbcOptionsInWrite(jdbcUrl, firstStepTempTable, sinkConnectionRefOptions),
            attDdl()
          )
        }
        loadedDF.write
          .format(sinkConnection.sparkFormat.getOrElse("jdbc"))
          .option("dbtable", firstStepTempTable)
          .mode(SaveMode.Append) // Because Overwrite loose the schema and require us to add quotes
          .options(sinkConnectionRefOptions)
          .save()

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
          new JdbcAutoTask(secondStepTaskDesc, Map.empty, None, truncate = false, test = test)(
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
          new JdbcAutoTask(secondStepDesc, Map.empty, None, truncate = false, test = test)
        secondAutoStepTask.updateJdbcTableSchema(loadedDF.schema, fullTableName)
        val jobResult = secondAutoStepTask.runJDBC(Some(loadedDF))
        jobResult
      }
    result
  }

}
