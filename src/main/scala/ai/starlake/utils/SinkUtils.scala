package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig, BigQuerySparkJob}
import ai.starlake.job.sink.jdbc.JdbcConnectionLoadConfig
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model._
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, DatasetLogging, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

class SinkUtils()(implicit settings: Settings) extends StrictLogging with DatasetLogging {

  def sinkInAudit(
    sinkType: ConnectionType,
    dataframe: DataFrame,
    table: String,
    maybeTableDescription: Option[String],
    /* arguments below used for filesink only */
    savePath: Path,
    lockPath: Path,
    storageHandler: StorageHandler,
    engine: Engine,
    session: SparkSession
  ): Try[Unit] = {
    // We sink to a file when running unit tests
    sinkType match {
      case ConnectionType.FS =>
        if (engine == Engine.SPARK) {
          val waitTimeMillis = settings.comet.lock.timeout
          val locker = new FileLock(lockPath, storageHandler)
          locker.tryExclusively(waitTimeMillis) {
            appendToFile(
              storageHandler,
              session,
              dataframe,
              savePath,
              settings.comet.audit.domain.getOrElse("audit"),
              table
            )
          }
        } else
          Success(())

      case ConnectionType.BQ =>
        Try {
          sinkToBigQuery(
            dataframe,
            settings.comet.audit.getDatabase(settings),
            settings.comet.audit.domain.getOrElse("audit"),
            table,
            maybeTableDescription,
            Some(
              settings.comet.audit.sink
                .getSink()
                .connectionRef
                .getOrElse(settings.comet.connectionRef)
            )
          )
        }

      case ConnectionType.ES =>
        // TODO Sink Expectations & Metrics to ES
        throw new Exception("Sinking Expectations & Metrics to Elasticsearch not yet supported")
      case ConnectionType.KAFKA =>
        // TODO Sink Expectations & Metrics to Kafka
        throw new Exception("Sinking Expectations & Metrics to Kafka not yet supported")
      case _ => // including SinkType.JDBC | SinkType.SNOWFLAKE | SinkType.REDSHIFT ect ...
        Try {
          val jdbcConfig = JdbcConnectionLoadConfig.fromComet(
            settings.comet.audit.sink
              .getSink()
              .connectionRef
              .getOrElse(settings.comet.connectionRef),
            settings.comet,
            Right(dataframe),
            (settings.comet.audit.domain.getOrElse("audit") + "." + table).toUpperCase()
          )
          sinkToJdbc(jdbcConfig)
        }
    }
  }

  private def sinkToBigQuery(
    dataframe: DataFrame,
    bqDatabase: Option[String],
    bqDataset: String,
    bqTable: String,
    maybeTableDescription: Option[String],
    connectionRef: Option[String] = None
  ): Unit = {
    if (dataframe.count() > 0) {
      val config = BigQueryLoadConfig(
        connectionRef,
        Right(dataframe),
        outputTableId =
          Some(BigQueryJobBase.extractProjectDatasetAndTable(bqDatabase, bqDataset, bqTable)),
        None,
        Nil,
        settings.comet.defaultFormat,
        "CREATE_IF_NEEDED",
        "WRITE_APPEND",
        None,
        outputDatabase = bqDatabase
      )
      val res = new BigQuerySparkJob(
        config,
        maybeSchema = Some(BigQuerySchemaConverters.toBigQuerySchema(dataframe.schema)),
        maybeTableDescription = maybeTableDescription
      ).run()
      res match {
        case Success(_) => ;
        case Failure(e) =>
          throw e
      }
    }
  }

  private def sinkToJdbc(
    cliConfig: JdbcConnectionLoadConfig
  ): Unit = {
    cliConfig.sourceFile match {
      case Left(_) =>
        throw new IllegalArgumentException("unsupported case with named source")
      case Right(dataframe) =>
        // TODO: SMELL: Refused Bequest
        require(
          cliConfig.writeDisposition == WriteDisposition.WRITE_APPEND,
          s"unsupported write disposition ${cliConfig.writeDisposition}, only WRITE_APPEND is supported"
        )

        val dfw = dataframe.write
          .format(cliConfig.format)
          .option("truncate", cliConfig.writeDisposition == WriteDisposition.WRITE_TRUNCATE)
          .option("dbtable", cliConfig.outputTable)

        logger.info(s"JDBC save done to table ${cliConfig.outputTable} at $cliConfig")

        dfw
          .options(cliConfig.options)
          .mode(SaveMode.Append)
          .save()
    }
  }

  /** Saves a dataset. If the path is empty (the first time we call metrics on the schema) then we
    * can write.
    *
    * If there's already parquet files stored in it, then create a temporary directory to compute
    * on, and flush the path to move updated metrics in it
    *
    * @param dataToSave
    *   : dataset to be saved
    * @param path
    *   : Path to save the file at
    */
  private def appendToFile(
    storageHandler: StorageHandler,
    session: SparkSession,
    dataToSave: DataFrame,
    path: Path,
    datasetName: String,
    tableName: String
  ): Unit = {
    if (storageHandler.exists(path)) {
      val pathIntermediate = new Path(path.getParent, ".tmp")

      logger.whenDebugEnabled {
        logger.debug(
          session.read
            .format(settings.comet.defaultFormat)
            .load(path.toString)
            .showString(truncate = 0)
        )
      }
      val dataByVariableStored: DataFrame = session.read
        .format(settings.comet.defaultFormat)
        .load(path.toString)
        .union(dataToSave)

      if (settings.comet.isHiveCompatible()) {
        val hiveDB = datasetName
        val fullTableName = s"$hiveDB.$tableName"
        session.sql(s"create database if not exists $hiveDB")
        session.sql(s"use $hiveDB")
        dataByVariableStored
          .repartition(1)
          .write
          .mode(SaveMode.Append)
          .format(settings.comet.defaultFormat)
          .saveAsTable(fullTableName)
      } else {
        dataByVariableStored
          .repartition(1)
          .write
          .mode(SaveMode.Append)
          .format(settings.comet.defaultFormat)
          .save(pathIntermediate.toString)
      }

      storageHandler.delete(path)
      storageHandler.move(pathIntermediate, path)
      logger.whenDebugEnabled {
        logger.debug(
          session.read
            .format(settings.comet.defaultFormat)
            .load(path.toString)
            .showString(1000, truncate = 0)
        )
      }
    } else {
      storageHandler.mkdirs(path)
      dataToSave
        .repartition(1)
        .write
        .mode(SaveMode.Append)
        .format(settings.comet.defaultFormat)
        .save(path.toString)
    }
  }
}
