package com.ebiznext.comet.job.bqload

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.job.conversion.bigquery._
import com.ebiznext.comet.job.conversion.syntax._
import com.ebiznext.comet.utils.{SparkJob, Utils}
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper
import com.google.cloud.bigquery.{Schema => BQSchema, _}
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTimePartitioning
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

class BigQueryLoadJob(
  cliConfig: BigQueryLoadConfig,
  maybeSchema: scala.Option[BQSchema] = None
)(implicit val settings: Settings)
    extends SparkJob {

  override def name: String = s"bqload-${cliConfig.outputDataset}-${cliConfig.outputTable}"

  val conf = session.sparkContext.hadoopConfiguration
  logger.info(s"BigQuery Config $cliConfig")

  val bigqueryHelper = RemoteBigQueryHelper.create
  val bigquery = bigqueryHelper.getOptions.getService

  val projectId = conf.get("fs.gs.project.id")
  val bucket = conf.get("fs.defaultFS")

  val tableId = TableId.of(cliConfig.outputDataset, cliConfig.outputTable)

  def getOrCreateDataset(): Dataset = {
    val datasetId = DatasetId.of(projectId, cliConfig.outputDataset)
    val dataset = scala.Option(bigquery.getDataset(datasetId))
    dataset.getOrElse {
      val datasetInfo = DatasetInfo
        .newBuilder(cliConfig.outputDataset)
        .setLocation(cliConfig.getLocation())
        .build
      bigquery.create(datasetInfo)
    }
  }

  def getOrCreateTable(df: DataFrame): Table = {
    getOrCreateDataset()
    import com.google.cloud.bigquery.{StandardTableDefinition, TableInfo}
    scala.Option(bigquery.getTable(tableId)) getOrElse {

      val tableDefinitionBuilder =
        cliConfig.outputPartition match {
          case Some(_) =>
            StandardTableDefinition.of(df.to[BQSchema]).toBuilder
          case _ => StandardTableDefinition.newBuilder()
        }

      cliConfig.outputPartition.foreach { outputPartition =>
        import com.google.cloud.bigquery.TimePartitioning
        val timeField =
          if (List("_PARTITIONDATE", "_PARTITIONTIME").contains(outputPartition))
            TimePartitioning
              .newBuilder(TimePartitioning.Type.DAY)
              .setRequirePartitionFilter(true)
          else
            TimePartitioning
              .newBuilder(TimePartitioning.Type.DAY)
              .setRequirePartitionFilter(true)
              .setField(outputPartition)

        val timeFieldWithExpiration = cliConfig.days
          .map(_ * 3600 * 24 * 1000L)
          .map(ms => timeField.setExpirationMs(ms))
          .getOrElse(timeField)
          .build()
        tableDefinitionBuilder.setTimePartitioning(timeFieldWithExpiration)
      }

      val tableDefinition = tableDefinitionBuilder.build()
      val tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build
      bigquery.create(tableInfo)
    }
  }

  def runBQSparkConnector(): Try[SparkSession] = {
    val conf = session.sparkContext.hadoopConfiguration
    logger.info(s"BigQuery Config $cliConfig")

    val bucket = conf.get("fs.gs.system.bucket")

    val inputPath = cliConfig.sourceFile
    logger.info(s"Input path $inputPath")

    logger.info(s"Temporary GCS path $bucket")
    session.conf.set("temporaryGcsBucket", bucket)

    val writeDisposition = JobInfo.WriteDisposition.valueOf(cliConfig.writeDisposition)
    val tableId = TableId.of(cliConfig.outputDataset, cliConfig.outputTable)
    val finalWriteDisposition = writeDisposition match {
      case JobInfo.WriteDisposition.WRITE_TRUNCATE =>
        logger.info(s"Deleting table $tableId")
        bigquery.delete(tableId)
        logger.info(s"Setting Write mode to Append")
        JobInfo.WriteDisposition.WRITE_APPEND
      case _ =>
        writeDisposition
    }
    Try {
      val sourceDF =
        inputPath match {
          case Left(path) => session.read.parquet(path)
          case Right(df)  => df
        }

      val table = getOrCreateTable(sourceDF)

      def bqPartition(): Unit = {
        conf.set(
          BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY,
          finalWriteDisposition.toString
        )
        conf.set(
          BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION_KEY,
          cliConfig.createDisposition
        )
        cliConfig.outputPartition.foreach { outputPartition =>
          import com.google.cloud.hadoop.repackaged.bigquery.com.google.api.services.bigquery.model.TimePartitioning
          val timeField =
            if (List("_PARTITIONDATE", "_PARTITIONTIME").contains(outputPartition))
              new TimePartitioning().setType("DAY").setRequirePartitionFilter(true)
            else
              new TimePartitioning()
                .setType("DAY")
                .setRequirePartitionFilter(true)
                .setField(outputPartition)
          val timePartitioning =
            new BigQueryTimePartitioning(
              timeField
            )

          conf.set(BigQueryConfiguration.OUTPUT_TABLE_PARTITIONING_KEY, timePartitioning.getAsJson)
        }
      }

      bqPartition()

      val bqTable = s"${cliConfig.outputDataset}.${cliConfig.outputTable}"

      val stdTableDefinition =
        bigquery.getTable(table.getTableId).getDefinition.asInstanceOf[StandardTableDefinition]
      logger.info(
        s"BigQuery Saving ${sourceDF.count()} rows to  ${table.getTableId} containing ${stdTableDefinition.getNumRows} rows"
      )
      sourceDF.write
        .mode(SaveMode.Append)
        .format("com.google.cloud.spark.bigquery")
        .option("table", bqTable)
        .save()

      val stdTableDefinitionAfter =
        bigquery.getTable(table.getTableId).getDefinition.asInstanceOf[StandardTableDefinition]
      logger.info(
        s"BigQuery Saved to ${table.getTableId} now contains ${stdTableDefinitionAfter.getNumRows} rows"
      )
      session
    }
  }

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): Try[SparkSession] = {
    val res = runBQSparkConnector()
    Utils.logFailure(res, logger)
  }

}
