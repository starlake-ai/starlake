package com.ebiznext.comet.job.index.bqload

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.{SparkJob, Utils}
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryException,
  BigQueryOptions,
  Clustering,
  JobInfo,
  StandardTableDefinition,
  Table,
  TableId,
  TableInfo,
  Schema => BQSchema
}
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import com.ebiznext.comet.utils.conversion.BigQueryUtils._
import com.ebiznext.comet.utils.conversion.syntax._

import scala.collection.JavaConverters._
import scala.util.Try

class BigQuerySparkJob(
  override val cliConfig: BigQueryLoadConfig,
  maybeSchema: Option[BQSchema] = None
)(implicit val settings: Settings)
    extends SparkJob
    with BigQueryJobBase {

  override def name: String = s"bqload-${cliConfig.outputDataset}-${cliConfig.outputTable}"

  val conf: Configuration = session.sparkContext.hadoopConfiguration
  logger.info(s"BigQuery Config $cliConfig")

  override val projectId: String = conf.get("fs.gs.project.id")

  val bucket: String = conf.get("fs.defaultFS")

  def prepareConf(): Configuration = {
    val conf = session.sparkContext.hadoopConfiguration
    logger.info(s"BigQuery Config $cliConfig")
    val bucket = Option(conf.get("fs.gs.system.bucket"))
    bucket.foreach { bucket =>
      logger.info(s"Temporary GCS path $bucket")
      session.conf.set("temporaryGcsBucket", bucket)
    }

    val writeDisposition = JobInfo.WriteDisposition.valueOf(cliConfig.writeDisposition)
    val finalWriteDisposition = (writeDisposition, cliConfig.outputPartition) match {
      case (JobInfo.WriteDisposition.WRITE_TRUNCATE, Some(partition)) =>
        logger.info(s"Overwriting partition $partition of Table $tableId")
        logger.info(s"Setting Write mode to Overwrite")
        JobInfo.WriteDisposition.WRITE_TRUNCATE
      case (JobInfo.WriteDisposition.WRITE_TRUNCATE, _) =>
        try {
          bigquery.delete(tableId)
        } catch {
          case e: BigQueryException =>
            // Log error and continue  (may be table does not exist)
            Utils.logException(logger, e)
        }
        logger.info(s"Setting Write mode to Append")
        JobInfo.WriteDisposition.WRITE_APPEND
      case _ =>
        writeDisposition
    }

    conf.set(
      BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY,
      finalWriteDisposition.toString
    )
    conf.set(
      BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION_KEY,
      cliConfig.createDisposition
    )
    conf
  }

  def getOrCreateTable(dataFrame: Option[DataFrame], maybeSchema: Option[BQSchema]): TableInfo = {
    getOrCreateDataset()
    Option(bigquery.getTable(tableId)) getOrElse {

      val withPartitionDefinition =
        (maybeSchema, cliConfig.outputPartition) match {
          case (Some(schema), Some(partitionField)) =>
            // Generating schema from YML to get the descriptions in BQ
            val partitioning =
              timePartitioning(partitionField, cliConfig.days, cliConfig.requirePartitionFilter)
                .build()
            StandardTableDefinition
              .newBuilder()
              .setSchema(schema)
              .setTimePartitioning(partitioning)
          case (Some(schema), None) =>
            // Generating schema from YML to get the descriptions in BQ
            StandardTableDefinition
              .newBuilder()
              .setSchema(schema)
          case (None, Some(partitionField)) =>
            // We would have loved to let BQ do the whole job (StandardTableDefinition.newBuilder())
            // But however seems like it does not work when there is an output partition
            val partitioning =
              timePartitioning(partitionField, cliConfig.days, cliConfig.requirePartitionFilter)
                .build()
            val tableDefinition =
              StandardTableDefinition
                .newBuilder()
                .setTimePartitioning(partitioning)
            dataFrame
              .map(dataFrame => tableDefinition.setSchema(dataFrame.to[BQSchema]))
              .getOrElse(tableDefinition)
          case (None, None) =>
            // In case of complex types, our inferred schema does not work, BQ introduces a list subfield, let him do the dirty job
            StandardTableDefinition
              .newBuilder()
        }

      val withClusteringDefinition =
        cliConfig.outputClustering match {
          case Nil =>
            withPartitionDefinition
          case fields =>
            import scala.collection.JavaConverters._
            val clustering = Clustering.newBuilder().setFields(fields.asJava).build()
            withPartitionDefinition.setClustering(clustering)
        }
      TableInfo.newBuilder(tableId, withClusteringDefinition.build()).build
    }
  }

  def runSparkConnector(): Try[Option[DataFrame]] = {
    prepareConf()
    Try {
      val cacheStorageLevel =
        settings.comet.internal.map(_.cacheStorageLevel).getOrElse(StorageLevel.MEMORY_AND_DISK)
      val sourceDF =
        cliConfig.source match {
          case Left(path) =>
            session.read
              .parquet(path)
              .persist(
                cacheStorageLevel
              )
          case Right(df) => df.persist(cacheStorageLevel)
        }

      val tableInfo = getOrCreateTable(Some(sourceDF), maybeSchema)
      val table = bigquery.create(tableInfo)

      val stdTableDefinition =
        bigquery.getTable(table.getTableId).getDefinition.asInstanceOf[StandardTableDefinition]
      logger.info(
        s"BigQuery Saving to  ${table.getTableId} containing ${stdTableDefinition.getNumRows} rows"
      )

      (cliConfig.writeDisposition, cliConfig.outputPartition) match {
        case ("WRITE_TRUNCATE", Some(partition)) =>
          logger.info(s"overwriting partition ${partition} in The BQ Table $bqTable")
          // BigQuery supports only this date format 'yyyyMMdd', so we have to use it
          // in order to overwrite only one partition
          val dateFormat = "yyyyMMdd"
          val partitions = sourceDF
            .select(date_format(col(partition), dateFormat).cast("string"))
            .where(col(partition).isNotNull)
            .distinct()
            .rdd
            .map(r => r.getString(0))
            .collect()

          partitions.foreach(partitionStr =>
            sourceDF
              .where(date_format(col(partition), dateFormat).cast("string") === partitionStr)
              .write
              .mode(SaveMode.Overwrite)
              .format("com.google.cloud.spark.bigquery")
              .option(
                "table",
                bqTable
              )
              .option("datePartition", partitionStr)
              .save()
          )
        case _ =>
          logger.info(s"Saving BQ Table $bqTable")
          sourceDF.write
            .mode(SaveMode.Append)
            .format("com.google.cloud.spark.bigquery")
            .option("table", bqTable)
            .save()
      }

      val stdTableDefinitionAfter =
        bigquery.getTable(table.getTableId).getDefinition.asInstanceOf[StandardTableDefinition]
      logger.info(
        s"BigQuery Saved to ${table.getTableId} now contains ${stdTableDefinitionAfter.getNumRows} rows"
      )

      prepareRLS().foreach { rlsStatement =>
        logger.info(s"Applying security $rlsStatement")
        try {
          Option(runJob(rlsStatement, cliConfig.getLocation())) match {
            case None =>
              throw new RuntimeException("Job no longer exists")
            case Some(job) if job.getStatus.getExecutionErrors() != null =>
              throw new RuntimeException(
                job.getStatus.getExecutionErrors().asScala.reverse.mkString(",")
              )
            case Some(job) =>
              logger.info(s"Job with id ${job} on Statement $rlsStatement succeeded")
          }

        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
      None
    }
  }

  /**
    * Just to force any spark job to implement its entry point within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): Try[Option[DataFrame]] = {
    val res = runSparkConnector()
    Utils.logFailure(res, logger)
  }

}

object BigQuerySparkJob {

  def getTable(
    session: SparkSession,
    datasetName: String,
    tableName: String
  ): Option[Table] = {
    val conf = session.sparkContext.hadoopConfiguration
    val projectId: String =
      Option(conf.get("fs.gs.project.id")).getOrElse(ServiceOptions.getDefaultProjectId)
    val bigquery: BigQuery = BigQueryOptions.getDefaultInstance().getService()
    val tableId = TableId.of(projectId, datasetName, tableName)
    Option(bigquery.getTable(tableId))
  }
}
