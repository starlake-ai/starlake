package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.exceptions.NullValueFoundException
import ai.starlake.schema.model.{ClusteringInfo, FieldPartitionInfo, TableInfo}
import ai.starlake.utils._
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  JobInfo,
  Schema => BQSchema,
  StandardTableDefinition,
  Table
}
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.google.cloud.hadoop.repackaged.gcs.com.google.auth.oauth2.GoogleCredentials
import com.google.common.io.BaseEncoding
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions.{col, date_format, sum, when}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.storage.StorageLevel

import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success, Try}

class BigQuerySparkJob(
  override val cliConfig: BigQueryLoadConfig,
  maybeSchema: Option[BQSchema] = None,
  maybeTableDescription: Option[String] = None
)(implicit val settings: Settings)
    extends SparkJob
    with BigQueryJobBase {

  lazy val connectorOptions =
    connectionOptions -- List("allowFieldAddition", "allowFieldRelaxation")

  override def name: String = s"bqload-${bqTable}"

  val conf: Configuration = session.sparkContext.hadoopConfiguration
  logger.info(s"BigQuery Config $cliConfig")

  val bucket: String = conf.get("fs.defaultFS")

  /** Prepare the configuration for the BigQuery connector
    */
  def prepareConf(): Configuration = {
    logger.info(s"BigQuery Config $cliConfig")
    // fs.default.name

    val bucketFromExtraConf = {
      connectorOptions
        .get("temporaryGcsBucket")
        .orElse(
          settings
            .storageHandler()
            .extraConf
            .get("temporaryGcsBucket")
        )
        .orElse(connectorOptions.get("gcsBucket"))
        .orElse(settings.storageHandler().extraConf.get("fs.gs.system.bucket"))
        .orElse(settings.storageHandler().extraConf.get("fs.default.name"))
    }

    val bucket: Option[String] =
      bucketFromExtraConf
        .orElse(Option(conf.get("fs.gs.system.bucket")))
        .orElse(Option(conf.get("fs.default.name")))

    bucket.foreach { bucket =>
      logger.info(s"Temporary GCS path $bucket")
      val prefix = bucket.indexOf("gs://")
      val bucketName = if (prefix >= 0) bucket.substring(prefix + "gs://".length) else bucket
      logger.info(s"Temporary GCS Name $bucketName")
      session.conf.set("temporaryGcsBucket", bucketName)
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
    // Authentication
    logger.info(s"Using ${connectionOptions("authType")} Credentials from GCS")
    connectionOptions("authType") match {
      case "APPLICATION_DEFAULT" =>
        val scopes = connectionOptions
          .getOrElse("authScopes", "https://www.googleapis.com/auth/cloud-platform")
          .split(',')
        val cred = GoogleCredentials
          .getApplicationDefault()
          .createScoped(scopes: _*)
        cred.refresh()
        val accessToken = cred.getAccessToken()
        session.conf.set("gcpAccessToken", accessToken.getTokenValue())
      case "SERVICE_ACCOUNT_JSON_KEYFILE" =>
        val jsonKeyContent = getJsonKeyContent()
        val jsonKeyInBase64 =
          BaseEncoding.base64.encode(jsonKeyContent.getBytes(StandardCharsets.UTF_8))
        session.conf.set("credentials", jsonKeyInBase64)
      case "SERVICE_ACCOUNT_JSON_KEY_BASE64" =>
        val jsonKeyInBase64 = connectionOptions("jsonKeyBase64")
        session.conf.set("credentials", jsonKeyInBase64)
      case "ACCESS_TOKEN" =>
        val accessToken = connectionOptions("gcpAccessToken")
        session.conf.set("gcpAccessToken", accessToken)
    }
    conf
  }

  def runSparkConnector(): Try[SparkJobResult] = {
    prepareConf()
    Try {
      val cacheStorageLevel =
        settings.appConfig.internal.map(_.cacheStorageLevel).getOrElse(StorageLevel.MEMORY_AND_DISK)
      cliConfig.source match {
        case Left(path) =>
          session.read
            .format(settings.appConfig.defaultFormat)
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
    }.flatMap { case (sourceDF, table) =>
      val stdTableDefinition =
        bigquery()
          .getTable(table.getTableId)
          .getDefinition[StandardTableDefinition]
      logger.info(
        s"BigQuery Saving to  ${table.getTableId} which contained ${stdTableDefinition.getNumRows} rows"
      )

      cliConfig.starlakeSchema.map { schema =>
        schema.attributes

      }
      val containsArrayOfRecords = cliConfig.starlakeSchema.exists(_.containsArrayOfRecords())
      val intermediateFormatSettings = settings.appConfig.internal.map(_.intermediateBigqueryFormat)
      val defaultIntermediateFormat = intermediateFormatSettings.getOrElse("parquet")

      val intermediateFormat =
        if (containsArrayOfRecords && defaultIntermediateFormat == "parquet")
          "orc"
        else
          defaultIntermediateFormat

      val partitionOverwriteMode =
        if (bqTable.endsWith("SL_BQ_TEST_DS.SL_BQ_TEST_TABLE_DYNAMIC"))
          "DYNAMIC" // Force during testing
        else
          cliConfig.dynamicPartitionOverwrite
            .map {
              case true  => "DYNAMIC"
              case false => "STATIC"
            }
            .getOrElse(
              session.conf.get("spark.sql.sources.partitionOverwriteMode", "STATIC").toUpperCase()
            )

      val output: Try[Long] =
        (cliConfig.writeDisposition, cliConfig.outputPartition, partitionOverwriteMode) match {
          case ("WRITE_TRUNCATE", Some(partitionField), "DYNAMIC") =>
            // Partitioned table
            logger.info(s"overwriting partition $partitionField in The BQ Table $bqTable")
            // BigQuery supports only this date format 'yyyyMMdd', so we have to use it
            // in order to overwrite only one partition
            val dateFormat = "yyyyMMdd"
            val (partitions, nullCountValues) = sourceDF
              .select(
                date_format(col(partitionField), dateFormat).cast("string").as(partitionField)
              )
              .groupBy(partitionField)
              .agg(
                sum(
                  when(col(partitionField).isNull, 1L)
                    .otherwise(0L)
                ).as("count_if_null")
              )
              .collect()
              .toList
              .foldLeft(List[String]() -> 0L) { case ((partitions, nullCount), row) =>
                val updatedPartitions = scala
                  .Option(row.getString(0)) match {
                  case Some(value) => value +: partitions
                  case None        => partitions
                }
                updatedPartitions -> (nullCount + row.getLong(1))
              }
            if (nullCountValues > 0 && settings.appConfig.rejectAllOnError) {
              logger.error("Null value found in partition")
              Failure(new NullValueFoundException(nullCountValues))
            } else {
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
                logger.info(s"Saving into partition $bqTable$$$partitionStr")
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
              Success(nullCountValues)
            }
          case (writeDisposition, _, partitionOverwriteMode) =>
            assert(
              partitionOverwriteMode == "STATIC" || partitionOverwriteMode == "DYNAMIC",
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
            logger.whenDebugEnabled {
              sourceDF.show()
            }
            val finalDF = sourceDF.write
              .mode(saveMode)
              .format("com.google.cloud.spark.bigquery")
              .option("table", bqTable)
              .option("intermediateFormat", intermediateFormat)
              .options(withFieldRelaxationOptions)
            finalDF.save()
            Success(0L)
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
      (cliConfig.sqlSource, maybeSchema) match {
        // TODO investigate difference between maybeSchema and starlakeSchema of cliConfig
        case (None, Some(bqSchema)) => // case of Load (Ingestion)
          val fieldsDescription = BigQuerySchemaConverters
            .toSpark(bqSchema)
            .fields
            .map(f => f.name -> f.getComment().getOrElse(""))
            .toMap[String, String]
          updateColumnsDescription(fieldsDescription)
        case (Some(_), Some(_)) =>
          throw new Exception(
            "Should never happen, SqlSource or TableSchema should be set exclusively"
          )
        case (_, None) => // case of a transformation job
        // Do nothing
      }
      // TODO verify if there is a difference between maybeTableDescription, schema.comment , task.desc
      updateTableDescription(table, maybeTableDescription.getOrElse(""))
      output.map(SparkJobResult(None, _))
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
