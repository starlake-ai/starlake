package com.ebiznext.comet.job.index.bqload

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.conversion.BigQueryUtils._
import com.ebiznext.comet.utils.{JobResult, SparkJob, SparkJobResult, Utils}
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  Clustering,
  JobInfo,
  Schema => BQSchema,
  StandardTableDefinition,
  Table,
  TableId,
  TableInfo
}
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

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

    conf.set(
      BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY,
      writeDisposition.toString
    )
    conf.set(
      BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION_KEY,
      cliConfig.createDisposition
    )
    conf
  }

  def getOrCreateTable(
    dataFrame: Option[DataFrame],
    maybeSchema: Option[BQSchema]
  ): (Table, StandardTableDefinition) = {
    getOrCreateDataset()

    val table = Option(bigquery.getTable(tableId)) getOrElse {
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
              .map(dataFrame => tableDefinition.setSchema(sparkToBq(dataFrame)))
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
            val clustering = Clustering.newBuilder().setFields(fields.asJava).build()
            withPartitionDefinition.setClustering(clustering)
        }
      bigquery.create(TableInfo.newBuilder(tableId, withClusteringDefinition.build()).build)
    }
    (table, table.getDefinition.asInstanceOf[StandardTableDefinition])

  }

  def runSparkConnector(): Try[SparkJobResult] = {
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

      val (table, tableDefinition) = getOrCreateTable(Some(sourceDF), maybeSchema)

      setTablePolicy(table)

      val stdTableDefinition =
        bigquery.getTable(table.getTableId).getDefinition.asInstanceOf[StandardTableDefinition]
      logger.info(
        s"BigQuery Saving to  ${table.getTableId} containing ${stdTableDefinition.getNumRows} rows"
      )

      val intermediateFormat =
        settings.comet.internal.map(_.intermediateBigqueryFormat).getOrElse("orc")
      val partitionOverwriteMode =
        session.conf
          .getOption("spark.sql.sources.partitionOverwriteMode")
          .getOrElse("static")
          .toLowerCase()

      (cliConfig.writeDisposition, cliConfig.outputPartition, partitionOverwriteMode) match {
        case ("WRITE_TRUNCATE", Some(partitionField), "dynamic") =>
          logger.info(s"overwriting partition ${partitionField} in The BQ Table $bqTable")
          // BigQuery supports only this date format 'yyyyMMdd', so we have to use it
          // in order to overwrite only one partition
          val dateFormat = "yyyyMMdd"
          val partitions = sourceDF
            .select(date_format(col(partitionField), dateFormat).cast("string"))
            .where(col(partitionField).isNotNull)
            .distinct()
            .rdd
            .map(r => r.getString(0))
            .collect()
            .toList

          cliConfig.partitionsToUpdate match {
            case None =>
              logger.info(
                s"No optimization applied -> the following ${partitions.length} partitions will be written: ${partitions
                  .mkString(",")}"
              )
            case Some(partitionsToUpdate) =>
              logger.info(
                s"After optimization -> only the following ${partitionsToUpdate.length} partitions will be written: ${partitionsToUpdate
                  .mkString(",")}"
              )
          }

          // Delete partitions becoming empty
          cliConfig.partitionsToUpdate match {
            case None =>
            case Some(partitionsToUpdate) =>
              partitionsToUpdate.foreach { partitionToUpdate =>
                // if partitionToUpdate is not in the list of parititions to merge. It means that it need to be deleted
                // this case happen mahen there is no more than a single element in the partition
                if (!partitions.contains(partitionToUpdate)) {
                  val emptyDF = session
                    .createDataFrame(session.sparkContext.emptyRDD[Row], sourceDF.schema)
                  emptyDF.write
                    .mode(SaveMode.Overwrite)
                    .format("com.google.cloud.spark.bigquery")
                    .option("datePartition", partitionToUpdate)
                    .option("table", bqTable)
                    .option("intermediateFormat", intermediateFormat)
                }
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
            val finalDFWithOptions = cliConfig.options
              .foldLeft(finalDF) { case (df, (k, v)) => df.option(k, v) }
            cliConfig.partitionsToUpdate match {
              case None =>
                finalDFWithOptions.save()
              case Some(partitionsToUpdate) =>
                // We only overwrite partitions containing updated or newly added elements
                if (partitionsToUpdate.contains(partitionStr)) {
                  logger.info(
                    s"Optimization -> Writing partition : $partitionStr"
                  )
                  finalDFWithOptions.save()
                } else {
                  logger.info(
                    s"Optimization -> Not writing partition : $partitionStr"
                  )
                }
            }
          }

        case (writeDisposition, _, "dynamic") =>
          logger.warn(
            "dynamic partition requested but not partition field defined. Ignoring dynamic option"
          )
          val saveMode =
            if (writeDisposition == "WRITE_TRUNCATE") SaveMode.Overwrite else SaveMode.Append
          logger.info(s"Saving BQ Table $bqTable")
          val finalDF = sourceDF.write
            .mode(saveMode)
            .format("com.google.cloud.spark.bigquery")
            .option("table", bqTable)
            .option("intermediateFormat", intermediateFormat)

          cliConfig.options.foldLeft(finalDF)((w, kv) => w.option(kv._1, kv._2)).save()

        case (writeDisposition, _, "static") =>
          val saveMode =
            if (writeDisposition == "WRITE_TRUNCATE") SaveMode.Overwrite else SaveMode.Append
          logger.info(s"Saving BQ Table $bqTable")
          val finalDF = sourceDF.write
            .mode(saveMode)
            .format("com.google.cloud.spark.bigquery")
            .option("table", bqTable)
            .option("intermediateFormat", intermediateFormat)

          cliConfig.options.foldLeft(finalDF)((w, kv) => w.option(kv._1, kv._2)).save()

        case (_, _, invalidPartitionOverwriteMode) =>
          throw new Exception(
            s"Only dynamic or static are values values for property partitionOverwriteMode. $invalidPartitionOverwriteMode found"
          )
      }

      val stdTableDefinitionAfter =
        bigquery.getTable(table.getTableId).getDefinition.asInstanceOf[StandardTableDefinition]
      logger.info(
        s"BigQuery Saved to ${table.getTableId} now contains ${stdTableDefinitionAfter.getNumRows} rows"
      )

      /** !!! We will use TABLE ACCESS CONTROLS as workaround, until RLS option is released !!!
        */
      //      prepareRLS().foreach { rlsStatement =>
      //        logger.info(s"Applying security $rlsStatement")
      //        try {
      //          Option(runJob(rlsStatement, cliConfig.getLocation())) match {
      //            case None =>
      //              throw new RuntimeException("Job no longer exists")
      //            case Some(job) if job.getStatus.getExecutionErrors() != null =>
      //              throw new RuntimeException(
      //                job.getStatus.getExecutionErrors().asScala.reverse.mkString(",")
      //              )
      //            case Some(job) =>
      //              logger.info(s"Job with id ${job} on Statement $rlsStatement succeeded")
      //          }
      //
      //        } catch {
      //          case e: Exception =>
      //            e.printStackTrace()
      //        }
      //      }
      SparkJobResult(None)
    }
  }

  private def setTablePolicy(table: Table) = {
    cliConfig.rls match {
      case Some(h :: Nil) => applyTableIamPolicy(table.getTableId, h)
      case _              => logger.info(s"Table ACL is not set on this Table: $tableId")
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
    session: SparkSession,
    datasetName: String,
    tableName: String
  ): TableMetadata = {
    val conf = session.sparkContext.hadoopConfiguration
    val projectId: String =
      Option(conf.get("fs.gs.project.id")).getOrElse(ServiceOptions.getDefaultProjectId)
    val bigquery: BigQuery = BigQueryOptions.getDefaultInstance().getService()
    val tableId = TableId.of(projectId, datasetName, tableName)
    TableMetadata(Option(bigquery.getTable(tableId)), bigquery)
  }
}
