package ai.starlake.job.transform

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.metrics.{ExpectationJob, SparkExpectationAssertionHandler}
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig, BigQuerySparkJob}
import ai.starlake.job.sink.es.{ESLoadConfig, ESLoadJob}
import ai.starlake.job.sink.jdbc.{sparkJdbcLoader, JdbcConnectionLoadConfig}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.{JobResult, SparkJobResult, Utils}
import better.files.File
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.PythonRunner
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

    val res = runSpark()
    res match {
      case Success(jobResult) if interactive.isEmpty =>
        sink(jobResult.dataframe)
      case Failure(_) =>
    }
    res
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

  val hiveDB = taskDesc.getHiveDB()
  val fullTableName = s"$hiveDB.${taskDesc.table}"

  private def sinkToFS(dataframe: DataFrame, sink: FsSink): Boolean = {
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

    val partitionedDFWriter =
      partitionedDatasetWriter(
        partitionedDF,
        sinkPartition.attributes
      )

    val clusteredDFWriter = sink.clustering match {
      case None          => partitionedDFWriter
      case Some(columns) => partitionedDFWriter.sortBy(columns.head, columns.tail: _*)
    }

    val finalDataset = clusteredDFWriter
      .mode(taskDesc.getWrite().toSaveMode)
      .format(sink.format.getOrElse(settings.appConfig.defaultFormat))
      .options(sink.getOptions())
      .option("path", targetPath.toString)

    if (settings.appConfig.isHiveCompatible()) {
      val tableName = taskDesc.table
      val hiveDB = taskDesc.getHiveDB()
      val fullTableName = s"$hiveDB.$tableName"
      session.sql(s"create database if not exists $hiveDB")
      session.sql(s"use $hiveDB")
      if (taskDesc.getWrite().toSaveMode == SaveMode.Overwrite && tableExists)
        session.sql(s"truncate table $tableName")
      finalDataset.saveAsTable(fullTableName)
      val tableTagPairs =
        Utils.extractTags(this.taskDesc.tags) + ("comment" -> taskDesc.comment.getOrElse(""))
      val tagsAsString = tableTagPairs.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
      session.sql(
        s"ALTER TABLE $fullTableName SET TBLPROPERTIES($tagsAsString)"
      )

      if (Utils.isRunningInDatabricks()) {
        taskDesc.attributesDesc.foreach { attrDesc =>
          session.sql(
            s"ALTER TABLE $tableName CHANGE COLUMN ${attrDesc}.name COMMENT '${attrDesc.comment}'"
          )
        }
      }
      analyze(fullTableName)
    } else {
      // TODO Handle SinkType.FS and SinkType to Hive in Sink section in the caller

      finalDataset.save()
      if (coalesce) {
        val extension =
          sink.extension.getOrElse(
            sink.format.getOrElse(settings.appConfig.defaultFormat)
          )
        val csvPath = storageHandler
          .list(targetPath, s".$extension", LocalDateTime.MIN, recursive = false)
          .head
        val finalPath = new Path(targetPath, targetPath.getName + s".$extension")
        storageHandler.move(csvPath, finalPath)
      }
    }
    true
  }

  /** For test purposes only
    *
    * @return
    */
  private def registerFSViews(): List[String] = {
    val acceptedPath = DatasetArea.accepted(".")
    val domains =
      if (storageHandler.exists(acceptedPath)) storageHandler.listDirectories(acceptedPath)
      else Nil
    domains.flatMap { domain =>
      val domainName = domain.getName
      val tables = storageHandler.listDirectories(domain)
      tables.flatMap { table =>
        Try {
          val tableName = table.getName
          logger.info(s"registering view for $domainName.$tableName with path $table")
          val tableDF = session.read
            .format(settings.appConfig.defaultFormat)
            .load(table.toString)
          tableDF.createOrReplaceTempView(s"$tableName")
          tableName
        }.toOption
      }
    }
  }

  private def sinkToES(): Boolean = {
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
        format = settings.appConfig.defaultFormat,
        domain = this.taskDesc.domain,
        schema = this.taskDesc.table,
        dataset = Some(Left(targetPath)),
        options = sink.getOptions()
      )

    val res = new ESLoadJob(esConfig, storageHandler, schemaHandler).run()

    Utils.logFailure(res, logger)
    res.isSuccess
  }

  def sink(maybeDataFrame: Option[DataFrame]): Boolean = {
    val sinkOption = this.sinkConfig
    logger.info(s"Spark Job succeeded. sinking data to $sinkOption")
    maybeDataFrame match {
      case None =>
        logger.info("No dataframe to sink. Sink done natively by to the source")
        true
      case Some(dataframe) =>
        sinkOption match {
          case Some(sink) =>
            sink match {
              case _: EsSink =>
                sinkToES()

              case fsSink: FsSink =>
                this.sinkToFS(dataframe, fsSink)

              case bqSink: BigQuerySink =>
                val source = Right(Utils.setNullableStateOfColumn(dataframe, nullable = true))
                val (createDisposition, writeDisposition) = {
                  Utils.getDBDisposition(
                    this.taskDesc.getWrite(),
                    hasMergeKeyDefined = false
                  )
                }
                val bqLoadConfig =
                  BigQueryLoadConfig(
                    connectionRef = Some(connectionRef),
                    source = source,
                    outputTableId = Some(
                      BigQueryJobBase.extractProjectDatasetAndTable(
                        this.taskDesc.database,
                        this.taskDesc.domain,
                        this.taskDesc.table
                      )
                    ),
                    sourceFormat = settings.appConfig.defaultFormat,
                    createDisposition = createDisposition,
                    writeDisposition = writeDisposition,
                    outputPartition = bqSink.timestamp,
                    outputClustering = bqSink.clustering.getOrElse(Nil),
                    days = bqSink.days,
                    requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
                    rls = this.taskDesc.rls,
                    acl = this.taskDesc.acl,
                    starlakeSchema = Some(Schema.fromTaskDesc(this.taskDesc)),
                    // outputTableDesc = action.taskDesc.comment.getOrElse(""),
                    sqlSource = None,
                    attributesDesc = this.taskDesc.attributesDesc,
                    outputDatabase = this.taskDesc.database
                  )
                val result =
                  new BigQuerySparkJob(bqLoadConfig, None, this.taskDesc.comment).run()
                result.isSuccess

              case jdbcSink: JdbcSink =>
                val jdbcName = connectionRef
                val source = Right(dataframe)
                val (createDisposition, writeDisposition) = {
                  Utils.getDBDisposition(
                    this.taskDesc.getWrite(),
                    hasMergeKeyDefined = false
                  )
                }
                val jdbcConfig = JdbcConnectionLoadConfig.fromComet(
                  jdbcName,
                  settings.appConfig,
                  source,
                  outputTable = this.taskDesc.domain + "." + this.taskDesc.table,
                  createDisposition = CreateDisposition.valueOf(createDisposition),
                  writeDisposition = WriteDisposition.valueOf(writeDisposition),
                  createTableIfAbsent = false
                )

                val res = new sparkJdbcLoader(jdbcConfig).run()
                res match {
                  case Success(_) => true
                  case Failure(e) => logger.error("JDBCLoad Failed", e); false
                }
              case _ =>
                logger.warn(
                  s"No supported Sink is activated for this job $sink, dumping to console"
                )
                dataframe.write.format("console").save()
                true
            }
          case None =>
            logger.warn("Sink is not activated for this job")
            true
        }
    }
  }

  private lazy val tableExists = {

    session.catalog.tableExists(taskDesc.domain, taskDesc.table)
  }

  def runSpark(): Try[SparkJobResult] = {
    val start = Timestamp.from(Instant.now())
    val res = Try {
      val localViews =
        if (
          sinkConfig
            .exists(_.isInstanceOf[FsSink]) && settings.appConfig.fileSystem.startsWith("file:")
        ) {
          // we are in local development mode pnly
          registerFSViews()
        } else {
          Nil
        }

      val dynamicPartitionOverwrite = None // Handled by Spark save options.
      val (preSql, sqlWithParameters, postSql, asTable) =
        buildAllSQLQueries(tableExists, dynamicPartitionOverwrite, None, localViews)
      preSql.foreach(req => session.sql(req))
      logger.info(s"""START COMPILE SQL $sqlWithParameters END COMPILE SQL""")
      logger.info(s"running sql request using ${taskDesc.getEngine()}")
      val dataframe = (taskDesc.sql, taskDesc.python) match {
        case (Some(sql), None) =>
          runSqlSpark(sqlWithParameters)
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
      dataframe match {
        case None =>
          SparkJobResult(None)
        case Some(dataframe) =>
          if (settings.appConfig.expectations.active) {
            new ExpectationJob(
              taskDesc.database,
              taskDesc.domain,
              taskDesc.table,
              taskDesc.expectations,
              storageHandler,
              schemaHandler,
              Some(Left(dataframe)),
              new SparkExpectationAssertionHandler(session)
            ).run()
          }

          postSql.foreach(req => session.sql(req))
          // Let us return the Dataframe so that it can be piped to another sink
          SparkJobResult(Some(dataframe))
      }

    }
    val end = Timestamp.from(Instant.now())
    res match {
      case Success(jobResult) =>
        val end = Timestamp.from(Instant.now())
        val jobResultCount = jobResult.dataframe match {
          case None => -1
          case Some(dataframe) =>
            dataframe.count()
        }
        applyHiveTableAcl()
        logAuditSuccess(start, end, jobResultCount)
      case Failure(e) =>
        logAuditFailure(start, end, e)
    }
    res
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

  private def runSqlSpark(sqlWithParameters: String): Option[DataFrame] = {
    val df =
      connection.getType() match {
        case ConnectionType.FS =>
          session.sql(sqlWithParameters)
        case _ =>
          session.read
            .format(connection.sparkFormat.getOrElse(throw new Exception("Should never happen")))
            .option("query", sqlWithParameters)
            .options(connection.options)
            .load()
      }
    Some(df)
  }
}
