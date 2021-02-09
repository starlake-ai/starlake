package com.ebiznext.comet.utils

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.job.index.bqload.{BigQueryLoadConfig, BigQuerySparkJob}
import com.ebiznext.comet.job.index.connectionload.ConnectionLoadConfig
import com.ebiznext.comet.schema.model._
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Success, Try}

class SinkUtils(implicit settings: Settings) extends StrictLogging {

  def sink(sinkType: Sink, dataframe: DataFrame, table: String): Try[Unit] = {
    sinkType match {
      case _: NoneSink =>
        Success(())

      case sink: BigQuerySink =>
        Try {
          sinkToBigQuery(dataframe, sink.name.getOrElse(table), table, sink.options)
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
            options = sink.options
          )
          sinkToJdbc(jdbcConfig)
        }
      case _: EsSink =>
        ???
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
        "parquet",
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

        cliConfig.options
          .foldLeft(dfw)((w, kv) => w.option(kv._1, kv._2))
          .mode(SaveMode.Append)
          .save()
    }
  }

}
