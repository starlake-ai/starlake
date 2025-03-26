package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.schema.model._
import ai.starlake.utils._
import com.google.cloud.bigquery.{JobInfo, Schema => BQSchema, StandardTableDefinition}
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.google.cloud.hadoop.repackaged.gcs.com.google.auth.oauth2.GoogleCredentials
import com.google.common.io.BaseEncoding
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel

import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

class BigQuerySparkJob(
  override val cliConfig: BigQueryLoadConfig,
  maybeBqSchema: Option[BQSchema] = None,
  maybeTableDescription: Option[String] = None,
  attributesDesc: List[AttributeDesc] = Nil
)(implicit val settings: Settings)
    extends SparkJob
    with BigQueryJobBase {

  lazy val connectorOptions =
    connectionOptions -- List("allowFieldAddition", "allowFieldRelaxation")

  override def name: String = {
    cliConfig.outputTableId match {
      case Some(_) => s"bqload-$bqTable"
      case _       => "bqload-query"
    }
  }

  val conf: Configuration = session.sparkContext.hadoopConfiguration
  logger.debug(s"BigQuery Config $cliConfig")

  val bucket: String = conf.get("fs.defaultFS")

  /** Prepare the configuration for the BigQuery connector
    */
  def prepareConf(): Configuration = {
    logger.debug(s"BigQuery Config $cliConfig")
    // fs.defaultFS

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
        .orElse(settings.storageHandler().extraConf.get("fs.defaultFS"))
    }

    val bucket: Option[String] =
      bucketFromExtraConf
        .orElse(Option(conf.get("fs.gs.system.bucket")))
        .orElse(Option(conf.get("fs.defaultFS")))

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
    cliConfig.accessToken match {
      case Some(accessToken) =>
        session.conf.set("gcpAccessToken", accessToken)
      case None =>
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
            val jsonKeyContent = BigQueryJobBase.getJsonKeyContent(connectionOptions)
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
    }
    conf
  }

  def runSparkWriter(): Try[SparkJobResult] = {
    prepareConf()
    Try {
      val cacheStorageLevel =
        settings.appConfig.internal.map(_.cacheStorageLevel).getOrElse(StorageLevel.MEMORY_AND_DISK)
      cliConfig.source match {
        case Left(path) =>
          session.read
            .format(settings.appConfig.defaultWriteFormat)
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
          maybeBqSchema,
          partitionField,
          clusteringFields,
          attributesDesc
        ),
        Some(sourceDF)
      )
        .map { case (table, _) => sourceDF -> table }
    }.flatMap { case (sourceDF, table) =>
      val stdTableDefinition =
        bigquery(accessToken = cliConfig.accessToken)
          .getTable(table.getTableId)
          .getDefinition[StandardTableDefinition]
      logger.info(
        s"BigQuery Saving to  ${table.getTableId} which contained ${stdTableDefinition.getNumRows} rows"
      )

      lazy val maybeStarlakeSchema = cliConfig.starlakeSchema
        .orElse(
          cliConfig.source.toOption.map(df =>
            Schema
              .fromSparkSchema("df_schema", StructField("ignore", df.schema))
          )
        )
      lazy val containsVariant = {
        maybeStarlakeSchema.exists(_.containsVariant())
      }
      lazy val containsArrayOfRecords = {
        maybeStarlakeSchema.exists(_.containsArrayOfRecords())
      }
      val intermediateFormatSettings = settings.appConfig.internal.map(_.intermediateBigqueryFormat)
      val pretendingIntermediateFormat = intermediateFormatSettings.getOrElse("parquet")

      val intermediateFormat = {
        if (containsVariant) {
          "avro"
        } else if (pretendingIntermediateFormat == "parquet" && containsArrayOfRecords)
          "orc"
        else
          pretendingIntermediateFormat
      }

      val output: Try[Long] =
        cliConfig.writeDisposition match {
          case writeDisposition =>
            val (saveMode, withFieldRelaxationOptions) =
              writeDisposition match {
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
                  throw new Exception(
                    s"Invalid write disposition $writeDisposition for table ${table.getTableId}"
                  )
              }
            logger.whenDebugEnabled {
              sourceDF.show()
            }

            // bigquery does not support having the cols in the wrong order
            val tableColNames: mutable.Seq[String] =
              stdTableDefinition.getSchema.getFields.asScala.map(_.getName)
            val fieldsMap = sourceDF.schema.fields.map { field => field.name -> field.name }.toMap
            val orderedFields = tableColNames.flatMap { fieldsMap.get }.toSeq
            val orderedDF = sourceDF.select(orderedFields.map(col): _*)
            orderedDF.write
              .mode(saveMode)
              .format("bigquery")
              .option("table", bqTable)
              .option("intermediateFormat", intermediateFormat)
              .options(withFieldRelaxationOptions)
              .save()
            Success(0L)
        }
      val stdTableDefinitionAfter =
        bigquery(accessToken = cliConfig.accessToken)
          .getTable(table.getTableId)
          .getDefinition[StandardTableDefinition]
      logger.info(
        s"BigQuery Saved to ${table.getTableId} now contains ${stdTableDefinitionAfter.getNumRows} rows"
      )
      val attributesDescMap = attributesDesc.map { case AttributeDesc(name, _, desc, _) =>
        name -> desc
      }.toMap

      if (attributesDescMap.nonEmpty)
        updateColumnsDescription(BigQueryJobBase.dictToBQSchema(attributesDescMap))
      // TODO verify if there is a difference between maybeTableDescription, schema.comment , task.desc
      updateTableDescription(table, maybeTableDescription.getOrElse(""))
      output.map(rejected => SparkJobResult(None, Some(IngestionCounters(0, 0, rejected, Nil))))
    }
  }

  def runSparkReader(sql: String): Try[DataFrame] = {
    val hasViewsEnabled =
      settings.sparkConfig.hasPath("datasource.bigquery.viewsEnabled")
    if (hasViewsEnabled) {
      prepareConf()
      Try {
        session.read.format("bigquery").load(sql)
      }
    } else {
      throw new Exception(
        "Make sure the key spark.datasource.bigquery.viewsEnabled is set in the application.sl.yml file."
      )
    }
  }

  /** Just to force any job to implement its entry point within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    val res = runSparkWriter()
    Utils.logFailure(res, logger)
  }

  def query(sql: String): Try[DataFrame] = {
    val res = runSparkReader(sql)
    Utils.logFailure(res, logger)
  }

}
