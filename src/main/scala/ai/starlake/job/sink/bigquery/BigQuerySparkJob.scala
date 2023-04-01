package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.schema.model.{ClusteringInfo, FieldPartitionInfo, TableInfo}
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult, Utils}
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  JobInfo,
  Schema => BQSchema,
  StandardTableDefinition,
  Table
}
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.storage.StorageLevel

import scala.util.Try

class BigQuerySparkJob(
  override val cliConfig: BigQueryLoadConfig,
  maybeSchema: Option[BQSchema] = None,
  maybeTableDescription: Option[String] = None
)(implicit val settings: Settings)
    extends SparkJob
    with BigQueryJobBase {

  val connectorOptions: Map[String, String] =
    cliConfig.options -- List("allowFieldAddition", "allowFieldRelaxation")

  override def name: String = s"bqload-${bqTable}"

  val conf: Configuration = session.sparkContext.hadoopConfiguration
  logger.info(s"BigQuery Config $cliConfig")

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

    conf.set(
      BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION.getKey(),
      writeDisposition.toString
    )
    conf.set(
      BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION.getKey(),
      cliConfig.createDisposition
    )
    conf
  }

  def runSparkConnector(): Try[SparkJobResult] = {
    prepareConf()
    Try {
      val cacheStorageLevel =
        settings.comet.internal.map(_.cacheStorageLevel).getOrElse(StorageLevel.MEMORY_AND_DISK)
      cliConfig.source match {
        case Left(path) =>
          session.read
            .format(settings.comet.defaultFormat)
            .load(path)
            .persist(
              cacheStorageLevel
            )
        case Right(df) => df.persist(cacheStorageLevel)
      }
    }.flatMap { sourceDF =>
      val partitionField = cliConfig.outputPartition.map { partitionField =>
        FieldPartitionInfo(partitionField, cliConfig.days, cliConfig.requirePartitionFilter)
      }
      val clusteringFields = cliConfig.outputClustering match {
        case Nil    => None
        case fields => Some(ClusteringInfo(fields.toList))
      }
      getOrCreateTable(
        cliConfig.domainDescription,
        TableInfo(
          tableId,
          maybeTableDescription,
          maybeSchema,
          partitionField,
          clusteringFields
        ),
        Some(sourceDF)
      )
        .map { case (table, _) => sourceDF -> table }
    }.map { case (sourceDF, table) =>
      val stdTableDefinition =
        bigquery()
          .getTable(table.getTableId)
          .getDefinition[StandardTableDefinition]
      logger.info(
        s"BigQuery Saving to  ${table.getTableId} containing ${stdTableDefinition.getNumRows} rows"
      )

      cliConfig.starlakeSchema.map { schema =>
        schema.attributes

      }
      val containsArrayOfRecords = cliConfig.starlakeSchema.exists(_.containsArrayOfRecords())
      val defaultIntermediateFormat =
        settings.comet.internal.map(_.intermediateBigqueryFormat).getOrElse("parquet")

      val intermediateFormat =
        if (containsArrayOfRecords && defaultIntermediateFormat == "parquet")
          "orc"
        else
          defaultIntermediateFormat

      val partitionOverwriteMode =
        session.conf.get("spark.sql.sources.partitionOverwriteMode", "static").toLowerCase()

      (cliConfig.writeDisposition, cliConfig.outputPartition, partitionOverwriteMode) match {
        case ("WRITE_TRUNCATE", Some(partitionField), "dynamic") =>
          // Partitioned table
          logger.info(s"overwriting partition $partitionField in The BQ Table $bqTable")
          // BigQuery supports only this date format 'yyyyMMdd', so we have to use it
          // in order to overwrite only one partition
          val dateFormat = "yyyyMMdd"
          val partitions = sourceDF
            .select(date_format(col(partitionField), dateFormat).cast("string"))
            .where(col(partitionField).isNotNull)
            .distinct()
            .collect()
            .toList
            .map(r => r.getString(0))

          cliConfig.partitionsToUpdate match {
            case Nil =>
              logger.info(
                s"No optimization applied -> the following ${partitions.length} partitions will be written: ${partitions
                    .mkString(",")}"
              )
            case partitionsToUpdate =>
              logger.info(
                s"After optimization -> only the following ${partitionsToUpdate.length} partitions will be written: ${partitionsToUpdate
                    .mkString(",")}"
              )
          }

          // Delete partitions becoming empty
          cliConfig.partitionsToUpdate.foreach { partitionToUpdate =>
            // if partitionToUpdate is not in the list of partitions to merge. It means that it need to be deleted
            // this case happen when there is no more than a single element in the partition
            if (!partitions.contains(partitionToUpdate)) {
              logger.info(s"Deleting partition $partitionToUpdate")
              val emptyDF = session
                .createDataFrame(session.sparkContext.emptyRDD[Row], sourceDF.schema)
              val finalEmptyDF = emptyDF.write
                .mode(SaveMode.Overwrite)
                .format("com.google.cloud.spark.bigquery")
                .option("datePartition", partitionToUpdate)
                .option("table", bqTable)
                .option("intermediateFormat", intermediateFormat)

              finalEmptyDF.options(connectorOptions).save()
            }
          }

          partitions.foreach { partitionStr =>
            val finalDF =
              sourceDF
                .where(
                  date_format(col(partitionField), dateFormat).cast("string") === partitionStr
                )
                .write
                .mode(SaveMode.Overwrite)
                .format("com.google.cloud.spark.bigquery")
                .option("datePartition", partitionStr)
                .option("table", bqTable)
                .option("intermediateFormat", intermediateFormat)
                .options(connectorOptions)
            cliConfig.partitionsToUpdate match {
              case Nil =>
                finalDF.save()
              case partitionsToUpdate =>
                // We only overwrite partitions containing updated or newly added elements
                if (partitionsToUpdate.contains(partitionStr)) {
                  logger.info(
                    s"Optimization -> Writing partition : $partitionStr"
                  )
                  finalDF.save()
                } else {
                  logger.info(
                    s"Optimization -> Not writing partition : $partitionStr"
                  )
                }
            }
          }
        case (writeDisposition, _, partitionOverwriteMode) =>
          assert(
            partitionOverwriteMode == "static" || partitionOverwriteMode == "dynamic",
            s"""Only dynamic or static are values values for property
                   |partitionOverwriteMode. $partitionOverwriteMode found""".stripMargin
          )

          val (saveMode, withFieldRelaxationOptions) = writeDisposition match {
            case "WRITE_TRUNCATE" => (SaveMode.Overwrite, connectorOptions)
            case _ if table.exists() =>
              (
                SaveMode.Append,
                connectorOptions ++ Map(
                  "allowFieldAddition"   -> "true",
                  "allowFieldRelaxation" -> "true"
                )
              )
            case _ =>
              (
                SaveMode.Append,
                connectorOptions
              )
          }

          val finalDF = sourceDF.write
            .mode(saveMode)
            .format("com.google.cloud.spark.bigquery")
            .option("table", bqTable)
            .option("intermediateFormat", intermediateFormat)
            .options(withFieldRelaxationOptions)
          finalDF.save()
      }

      val stdTableDefinitionAfter =
        bigquery()
          .getTable(table.getTableId)
          .getDefinition[StandardTableDefinition]
      logger.info(
        s"BigQuery Saved to ${table.getTableId} now contains ${stdTableDefinitionAfter.getNumRows} rows"
      )
      applyRLSAndCLS().recover { case e =>
        Utils.logException(logger, e)
        throw e
      }
      updateColumnsDescription(cliConfig.sqlSource.getOrElse(""))

      SparkJobResult(None)
    }
  }

  /** Just to force any spark job to implement its entry point within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    val res = runSparkConnector()
    Utils.logFailure(res, logger)
  }

}

case class TableMetadata(table: Option[Table], biqueryClient: BigQuery)

object BigQuerySparkJob {

  def getTable(
    resourceId: String
  ): TableMetadata = {
    val finalTableId = BigQueryJobBase.extractProjectDatasetAndTable(resourceId)
    val bigquery = BigQueryOptions.getDefaultInstance().getService()
    TableMetadata(Option(bigquery.getTable(finalTableId)), bigquery)
  }

}
