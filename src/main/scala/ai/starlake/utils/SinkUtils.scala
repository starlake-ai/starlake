package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.{BigQueryLoadConfig, BigQuerySparkJob}
import ai.starlake.job.sink.jdbc.ConnectionLoadConfig
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model._
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, DatasetLogging, SaveMode, SparkSession}

import scala.util.{Success, Try}

class SinkUtils(implicit settings: Settings) extends StrictLogging with DatasetLogging {

  def sink(
    sinkType: Sink,
    dataframe: DataFrame,
    table: String,
    /* arguments below used for filesink only */
    savePath: Path,
    lockPath: Path,
    storageHandler: StorageHandler,
    engine: Engine,
    session: SparkSession
  ): Try[Unit] = {
    // We sink to a file when running unit tests
    if (settings.comet.sinkToFile && engine == Engine.SPARK) {
      val waitTimeMillis = settings.comet.lock.timeout
      val locker = new FileLock(lockPath, storageHandler)
      locker.tryExclusively(waitTimeMillis) {
        appendToFile(
          storageHandler,
          session,
          dataframe,
          savePath,
          sinkType.name.getOrElse(table),
          table
        )
      }
    }
    sinkType match {
      case _: NoneSink | FsSink(_, _, _, _, _) if !settings.comet.sinkToFile =>
        if (engine == Engine.SPARK) {
          val waitTimeMillis = settings.comet.lock.timeout
          val locker = new FileLock(lockPath, storageHandler)
          locker.tryExclusively(waitTimeMillis) {
            appendToFile(
              storageHandler,
              session,
              dataframe,
              savePath,
              sinkType.name.getOrElse(table),
              table
            )
          }
        } else
          Success(())

      case _: NoneSink | FsSink(_, _, _, _, _) if settings.comet.sinkToFile =>
        // Do nothing dataset already sinked to file. Forced at the reference.conf level
        Success(())

      case sink: BigQuerySink =>
        Try {
          sinkToBigQuery(dataframe, sink.name.getOrElse(table), table, sink.getOptions)
        }

      case sink: JdbcSink =>
        Try {
          val jdbcConfig = ConnectionLoadConfig.fromComet(
            sink.connection,
            settings.comet,
            Right(dataframe),
            table,
            partitions = sink.partitions.getOrElse(1),
            batchSize = sink.batchsize.getOrElse(1000),
            options = sink.getOptions
          )
          sinkToJdbc(jdbcConfig)
        }
      case _: EsSink =>
        // TODO Sink Assertions & Metrics to ES
        throw new Exception("Sinking Assertions & Metrics to Elasticsearch not yet supported")
    }
  }

  private def sinkToBigQuery(
    dataframe: DataFrame,
    bqDataset: String,
    bqTable: String,
    options: Map[String, String]
  ): Unit = {
    if (dataframe.count() > 0) {
      val config = BigQueryLoadConfig(
        Right(dataframe),
        outputDataset = bqDataset,
        outputTable = bqTable,
        None,
        Nil,
        settings.comet.defaultFormat,
        "CREATE_IF_NEEDED",
        "WRITE_APPEND",
        None,
        None,
        options = options
      )
      // Do not pass the schema here. Not that we do not compute the schema correctly
      // But since we are having a record of repeated field BQ does not like
      // the way we pass the schema. BQ needs an extra "list" subfield for repeated fields
      // So let him determine teh schema by himself or risk tonot to be able to append the metrics
      val res = new BigQuerySparkJob(config).run()
      Utils.logFailure(res, logger)
    }
  }

  private def sinkToJdbc(
    cliConfig: ConnectionLoadConfig
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
          .format("jdbc")
          .option("truncate", cliConfig.writeDisposition == WriteDisposition.WRITE_TRUNCATE)
          .option("dbtable", cliConfig.outputTable)

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
  protected def appendToFile(
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

      if (settings.comet.hive) {
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
