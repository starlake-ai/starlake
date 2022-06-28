package ai.starlake.job.sink.kafka

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Formatter._
import ai.starlake.utils.kafka.KafkaClient
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult, Utils}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.hadoop.fs.Path
import org.apache.kafka.common.serialization.Deserializer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.streaming.Trigger

import scala.collection.JavaConverters._
import scala.util.Try

object CustomDeserializer {
  var userDefinedDeserializer: Deserializer[Any] = _

  def configure(customDeserializerName: String, configs: Map[String, _]): Unit = {
    userDefinedDeserializer = Class
      .forName(customDeserializerName)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[Deserializer[Any]]

    userDefinedDeserializer.configure(configs.asJava, false)
  }

  def deserialize(topic: String, bytes: Array[Byte]): String =
    userDefinedDeserializer.deserialize(topic, bytes).toString

}

class KafkaJob(
  val kafkaJobConfig: KafkaJobConfig
)(implicit val settings: Settings)
    extends SparkJob {
  import settings.metadataStorageHandler
  DatasetArea.initMetadata(metadataStorageHandler)
  val schemaHandler = new SchemaHandler(metadataStorageHandler)

  private val topicConfig: Settings.KafkaTopicConfig =
    settings.comet.kafka.topics(kafkaJobConfig.topicConfigName)

  private val finalPath = kafkaJobConfig.path.richFormat(
    schemaHandler.activeEnv,
    Map("config" -> kafkaJobConfig.topicConfigName, "topic" -> topicConfig.topicName)
  )

  val schemaRegistryUrl: Option[JdbcConfigName] =
    settings.comet.kafka.serverOptions.get("schema.registry.url")

  val schemaRegistryClient: Option[CachedSchemaRegistryClient] =
    schemaRegistryUrl.map(schemaRegistryUrl =>
      new CachedSchemaRegistryClient(
        schemaRegistryUrl,
        128,
        settings.comet.kafka.serverOptions.asJava
      )
    )

  def lookupTopicSchema(topic: String, isKey: Boolean = false): Option[JdbcConfigName] = {
    schemaRegistryClient.map(
      _.getLatestSchemaMetadata(topic + (if (isKey) "-key" else "-value")).getSchema
    )
  }

  def avroSchemaToSparkSchema(avroSchema: String): SchemaConverters.SchemaType = {
    val parser = new org.apache.avro.Schema.Parser
    SchemaConverters.toSqlType(parser.parse(avroSchema))
  }

  private val dfValueSchema: Option[SchemaConverters.SchemaType] = {
    val rawSchema = lookupTopicSchema(topicConfig.topicName)
    rawSchema.map(rawSchema => avroSchemaToSparkSchema(rawSchema))
  }

  private val writeOptions = kafkaJobConfig.writeOptions.get("config") match {
    case Some(configValue) =>
      loadOptionsFromConfig(configValue)
    case None =>
      kafkaJobConfig.writeOptions
  }

  private val options = kafkaJobConfig.options.get("config") match {
    case Some(configValue) =>
      loadOptionsFromConfig(configValue)
    case None =>
      kafkaJobConfig.options
  }

  private def loadOptionsFromConfig(configValue: String): Map[String, String] = {
    settings.extraConf
      .getConfig(configValue)
      .entrySet()
      .asScala
      .to[Vector]
      .map(x => (x.getKey, x.getValue.unwrapped().toString))
      .toMap
  }

  def offload(): Try[SparkJobResult] = {
    Try {
      if (!kafkaJobConfig.streaming) {
        Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
          val (df, offsets) = kafkaClient
            .consumeTopicBatch(
              kafkaJobConfig.topicConfigName,
              session,
              topicConfig
            )

          val transformedDF: DataFrame = batchSave(df)

          kafkaClient.topicSaveOffsets(
            kafkaJobConfig.topicConfigName,
            topicConfig.allAccessOptions(settings.comet.kafka.sparkServerOptions),
            offsets
          )
          SparkJobResult(Some(transformedDF))
        }
      } else {
        Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
          val df = kafkaClient
            .consumeTopicStreaming(
              session,
              topicConfig
            )
          streamToKafka(df)
          SparkJobResult(None)
        }
      }
    }
  }

  def load(): Try[SparkJobResult] = {
    Try {
      Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
        // DataSource.lookupDataSource(kafkaJobConfig.format, session.sessionState.conf)

        kafkaJobConfig.streaming match {
          case true =>
            val df = session.readStream
              .format(kafkaJobConfig.format)
              .options(options)
              .load()
              .selectExpr(topicConfig.fields: _*)
            streamToKafka(df)
            SparkJobResult(None)
          case false =>
            val df = session.read.format(kafkaJobConfig.format).load(finalPath.split(','): _*)
            val transformedDF = transfom(df)

            kafkaClient.sinkToTopic(
              topicConfig,
              transformedDF
            )
            SparkJobResult(Some(transformedDF))
        }
      }
    }
  }

  private def batchSave(df: DataFrame) = {
    val transformedDF = transfom(df)
    val finalDF =
      kafkaJobConfig.coalesce match {
        case None    => transformedDF
        case Some(x) => transformedDF.repartition(x)
      }

    logger.info(s"Saving to $kafkaJobConfig")
    finalDF.write
      .mode(kafkaJobConfig.mode)
      .format(kafkaJobConfig.format)
      .options(writeOptions)
      .save(finalPath)
    logger.info(s"Kafka saved messages to offload -> ${finalPath}")

    kafkaJobConfig.coalesce match {
      case Some(1) =>
        val targetPath = new Path(finalPath)
        val singleFile = settings.storageHandler
          .list(
            targetPath,
            recursive = false
          )
          .filter(_.getName.startsWith("part-"))
          .head
        val tmpPath = new Path(targetPath.toString + ".tmp")
        if (settings.storageHandler.move(singleFile, tmpPath)) {
          settings.storageHandler.delete(targetPath)
          settings.storageHandler.move(tmpPath, targetPath)
        }
      case _ =>
    }
    transformedDF
  }

  private def streamToKafka(df: DataFrame) = {
    val transformedDF = transfom(df)

    val writer = transformedDF.writeStream
      .outputMode(kafkaJobConfig.streamingWriteMode)
      .format(kafkaJobConfig.streamingWriteFormat)
      .options(writeOptions)

    val trigger = kafkaJobConfig.streamingTrigger.map(_.toLowerCase).map {
      case "once"           => Trigger.Once()
      case "processingtime" => Trigger.ProcessingTime(kafkaJobConfig.streamingTriggerOption)
      case "continuous"     => Trigger.Continuous(kafkaJobConfig.streamingTriggerOption)
    }

    val triggerWriter = trigger match {
      case Some(trigger) => writer.trigger(trigger)
      case None          => writer
    }

    val partitionedWriter = kafkaJobConfig.streamingWritePartitionBy match {
      case Nil =>
        triggerWriter
      case list =>
        triggerWriter.partitionBy(list: _*)
    }
    val streamingQuery =
      if (kafkaJobConfig.streamingWriteToTable) // partitionedWriter.toTable(kafkaJobConfig.path)
        throw new Exception("streamingWriteToTable Not Supported")
      else
        partitionedWriter
          .start(finalPath)

    streamingQuery
      .awaitTermination()
  }

  private val transformInstance: Option[DataFrameTransform] = {
    kafkaJobConfig.transform
      .map(Utils.loadInstance[DataFrameTransform])
      .map(_.configure(topicConfig))
  }

  private def transfom(df: DataFrame): DataFrame = {
    val transformedDF = transformInstance match {
      case Some(transformer) =>
        transformer.transform(df, session)
      case None =>
        df
    }
    transformedDF
  }

  override def run(): Try[JobResult] = {
    settings.comet.kafka.customDeserializer.foreach { customDeserializerName =>
      val options =
        settings.comet.kafka.serverOptions
      CustomDeserializer.configure(customDeserializerName, options)
      val topicName = topicConfig.topicName
      session.udf.register(
        "deserialize",
        (bytes: Array[Byte]) => CustomDeserializer.deserialize(topicName, bytes)
      )

    }
    if (kafkaJobConfig.offload) {
      offload()
    } else {
      load()
    }
  }

  override def name: String = s"${kafkaJobConfig.topicConfigName}"
}
