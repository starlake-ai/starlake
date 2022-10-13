package ai.starlake.job.sink.kafka

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.sink.DataFrameTransform
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

class KafkaJob(
  val kafkaJobConfig: KafkaJobConfig
)(implicit val settings: Settings)
    extends SparkJob {
  import settings.metadataStorageHandler
  DatasetArea.initMetadata(metadataStorageHandler)
  val schemaHandler = new SchemaHandler(metadataStorageHandler)

  private val topicConfig: Option[Settings.KafkaTopicConfig] =
    kafkaJobConfig.topicConfigName.map(settings.comet.kafka.topics)

  private val writeTopicConfig: Option[Settings.KafkaTopicConfig] =
    kafkaJobConfig.writeTopicConfigName.map(settings.comet.kafka.topics)

  private val finalWritePath: Option[JdbcConfigName] = formatPath(kafkaJobConfig.writePath)

  private val finalLoadPath: Option[JdbcConfigName] = formatPath(kafkaJobConfig.path)

  private def formatPath(path: Option[String]): Option[String] = path
    .map(
      _.richFormat(
        schemaHandler.activeEnv(),
        Map(
          "config" -> kafkaJobConfig.topicConfigName.getOrElse(""),
          "topic"  -> topicConfig.map(_.topicName).getOrElse("")
        )
      )
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

  private val writeOptions = kafkaJobConfig.writeOptions.get("config") match {
    case Some(configValue) if kafkaJobConfig.writeFormat == "kafka" =>
      settings.comet.kafka
        .topics(configValue)
        .allAccessOptions() ++ (kafkaJobConfig.writeOptions - "config")
    case Some(configValue) =>
      loadOptionsFromConfig(configValue) ++ (kafkaJobConfig.writeOptions - "config")
    case None =>
      kafkaJobConfig.writeOptions
  }

  private val options = kafkaJobConfig.options.get("config") match {
    case Some(configValue) if kafkaJobConfig.format == "kafka" =>
      settings.comet.kafka
        .topics(configValue)
        .allAccessOptions() ++ (kafkaJobConfig.options - "config")
    case Some(configValue) =>
      loadOptionsFromConfig(configValue) ++ (kafkaJobConfig.options - "config")
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

  def pipeline(): Try[SparkJobResult] = {
    Try {
      topicConfig match {
        case Some(topicConfig) =>
          if (kafkaJobConfig.streaming) {
            val df = KafkaClient.consumeTopicStreaming(
              session,
              topicConfig
            )
            val transformedDF = transform(df)
            writeStreaming(transformedDF)
            SparkJobResult(None)
          } else {
            Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
              val (df, offsets) = kafkaClient
                .consumeTopicBatch(
                  kafkaJobConfig.topicConfigName.getOrElse(""),
                  session,
                  topicConfig
                )
              val transformedDF = transform(df)

              val savedDF: DataFrame = batchSave(transformedDF)

              kafkaClient.topicSaveOffsets(
                kafkaJobConfig.topicConfigName.getOrElse(""),
                topicConfig.allAccessOptions(),
                offsets
              )
              SparkJobResult(Some(savedDF))
            }
          }
        case None =>
          if (kafkaJobConfig.streaming) {
            assert(kafkaJobConfig.format != "kafka")
            val df = session.readStream.format(kafkaJobConfig.format).options(options).load()
            val transformedDF = transform(df)
            writeStreaming(transformedDF)
            SparkJobResult(None)
          } else {
            assert(kafkaJobConfig.path.isDefined)
            val df = session.read
              .format(kafkaJobConfig.format)
              .load(
                finalLoadPath
                  .getOrElse(throw new Exception("Load path should be set in config"))
                  .split(','): _*
              )
            val transformedDF: DataFrame = transform(df)
            (kafkaJobConfig.writeFormat, writeTopicConfig) match {
              case ("kafka", Some(writeTopicConfig)) =>
                Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
                  kafkaClient.sinkToTopic(
                    writeTopicConfig,
                    transformedDF
                  )
                }
              case _ =>
                batchSave(transformedDF)
            }
            SparkJobResult(Some(transformedDF))
          }
      }
    }
  }

  private def batchSave(df: DataFrame) = {
    val finalDF =
      kafkaJobConfig.coalesce match {
        case None    => df
        case Some(x) => df.repartition(x)
      }

    logger.info(s"Saving to $kafkaJobConfig")
    val kafkaOptions =
      if (kafkaJobConfig.writeFormat == "kafka")
        writeTopicConfig.map(_.allAccessOptions()).getOrElse(Map.empty)
      else
        Map.empty[JdbcConfigName, JdbcConfigName]

    val dfWriter = finalDF.write
      .mode(kafkaJobConfig.writeMode)
      .format(kafkaJobConfig.writeFormat)
      .options(kafkaOptions ++ writeOptions)

    finalWritePath match {
      case None =>
        dfWriter.save()
      case Some(path) =>
        dfWriter.save(path)
    }

    logger.info(s"Kafka saved messages to offload -> ${finalWritePath}")

    (kafkaJobConfig.coalesce, finalWritePath) match {
      case (Some(1), Some(path)) =>
        val targetPath = new Path(path)
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
      case (None, _) =>
      case (_, _) =>
        throw new Exception("Only coalesce(1) supported. Anything else is ignored")
    }
    df
  }

  private def transform(df: DataFrame) = {
    val transformedDF = DataFrameTransform.transform(transformInstance, df, session)
    transformedDF
  }

  private def writeStreaming(df: DataFrame) = {

    val writer = {
      (kafkaJobConfig.writeFormat, writeTopicConfig) match {
        case ("kafka", Some(writeTopicConfig)) =>
          df.writeStream
            .outputMode(kafkaJobConfig.writeMode)
            .options(writeTopicConfig.allAccessOptions())
            .option("topic", writeTopicConfig.topicName)
            .format(kafkaJobConfig.writeFormat)
            .options(writeOptions)
        case (_, None) =>
          // ("kafka", None) is accepted because we can set the topic & brokers config in the writeOptions
          df.writeStream
            .outputMode(kafkaJobConfig.writeMode)
            .format(kafkaJobConfig.writeFormat)
            .options(writeOptions)
        case (_, Some(_)) =>
          throw new Exception(
            "Cannot load to destination not kafka  with topic config name"
          )
      }
    }

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
      (finalWritePath, kafkaJobConfig.streamingWriteToTable) match {
        case (_, true) =>
          throw new Exception("streamingWriteToTable Not Supported")
        case (Some(path), false) =>
          partitionedWriter
            .start(path)
        case (None, false) =>
          partitionedWriter.start()
      }

    streamingQuery
      .awaitTermination()
  }

  private val transformInstance: Option[DataFrameTransform] = {
    kafkaJobConfig.transform
      .map(Utils.loadInstance[DataFrameTransform])
  }

  override def run(): Try[JobResult] = {
    val customDeserializers = settings.comet.kafka.customDeserializers.getOrElse(Map.empty)
    customDeserializers.foreach { case (customDeserializerName, customDeserializerFunction) =>
      val topicName = topicConfig
        .map(_.topicName)
        .getOrElse(
          writeTopicConfig
            .map(_.topicName)
            .getOrElse(throw new Exception("Cannot register de/serializers if topic not defined"))
        )
      val userDefinedDeserializer =
        CustomDeserializer.configure(customDeserializerFunction, settings.comet.kafka.serverOptions)

      session.udf.register(
        customDeserializerName,
        (bytes: Array[Byte]) =>
          CustomDeserializer.deserialize(userDefinedDeserializer, topicName, bytes)
      )
    }
    pipeline()
  }
  override def name: String = s"${kafkaJobConfig.topicConfigName}"
}

object CustomDeserializer {
  def configure(customDeserializerName: String, configs: Map[String, _]): Deserializer[Any] = {
    val userDefinedDeserializer = Class
      .forName(customDeserializerName)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[Deserializer[Any]]
    userDefinedDeserializer.configure(configs.asJava, false)
    userDefinedDeserializer
  }

  def deserialize(
    userDefinedDeserializer: Deserializer[Any],
    topic: String,
    bytes: Array[Byte]
  ): String =
    userDefinedDeserializer.deserialize(topic, bytes).toString
}
