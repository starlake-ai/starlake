package ai.starlake.utils.kafka

import ai.starlake.config.Settings
import ai.starlake.config.Settings.{KafkaConfig, KafkaTopicConfig}
import ai.starlake.schema.model.Mode
import ai.starlake.utils.{FileLock, YamlSerializer}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.{TopicPartition, TopicPartitionInfo}
import org.apache.spark.sql.{DataFrame, DatasetLogging, SparkSession}

import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class KafkaClient(kafkaConfig: KafkaConfig)(implicit settings: Settings)
    extends StrictLogging
    with DatasetLogging
    with AutoCloseable {

  val cometOffsetsMode: Mode =
    settings.comet.kafka.cometOffsetsMode.map(Mode.fromString).getOrElse(Mode.STREAM)
  val serverOptions: Map[String, String] = kafkaConfig.serverOptions
  val cometOffsetsConfig: KafkaTopicConfig = kafkaConfig.topics("comet_offsets")

  val serverOptionsProperties = new Properties()
  serverOptions.foreach { case (k, v) =>
    serverOptionsProperties.put(k, v)
  }

  lazy val client: AdminClient = AdminClient.create(serverOptionsProperties)

  cometOffsetsMode match {
    case Mode.STREAM =>
      createTopicIfNotPresent(
        new NewTopic(
          cometOffsetsConfig.topicName,
          cometOffsetsConfig.partitions,
          cometOffsetsConfig.replicationFactor
        ),
        Map("cleanup.policy" -> "compact")
      )
    case Mode.FILE =>
      if (!settings.storageHandler.exists(new Path(cometOffsetsConfig.topicName)))
        settings.storageHandler.mkdirs(new Path(cometOffsetsConfig.topicName))
    case _ =>
      throw new Exception("Should never happen")
  }

  def close(): Unit = client.close()

  def deleteTopic(topicName: String): Unit = {
    val found = client.listTopics().names().get().contains(topicName)
    logger.info(client.listTopics().names().get().asScala.toSet.mkString("\n"))
    if (found) {
      logger.info(s"Deleting topic $topicName")
      client.deleteTopics(List(topicName).asJavaCollection)
    }
  }

  def createTopicIfNotPresent(topic: NewTopic, conf: Map[String, String]): Unit = {
    val found = client.listTopics().names().get().contains(topic.name)
    if (!found) {
      client.createTopics(java.util.Collections.singleton(topic.configs(conf.asJava))).all().get()
    }
  }

  def topicEndOffsets(topicName: String, accessOptions: Map[String, String]): List[(Int, Long)] = {
    Try {
      val consumer = newConsumer(topicName, accessOptions)
      val partitions = extractPartitions(topicName, consumer)
      consumer.assign(partitions.asJava)
      consumer.seekToEnd(partitions.asJava)
      val result = partitions.map(p => (p.partition(), consumer.position(p)))
      consumer.close()
      result
    } match {
      case Failure(e) =>
        e.printStackTrace()
        throw e
      case Success(value) => value
    }
  }

  private def extractPartitions(topicName: String, consumer: KafkaConsumer[String, String]) = {
    val partitions = consumer
      .partitionsFor(topicName)
      .asScala
      .map(info => new TopicPartition(topicName, info.partition()))
      .toList
    partitions
  }

  private def newConsumer(topicName: String, accessOptions: Map[String, String]) = {
    val props: Properties = buildProps(accessOptions)
    logger.whenInfoEnabled {
      import scala.collection.JavaConverters._
      logger.info(s"access options for topic $topicName ==>")
      props.asScala.foreach { case (k, v) =>
        logger.info(s"\t$k=$v")
      }
    }
    val consumer = new KafkaConsumer[String, String](props)
    consumer
  }

  private def buildProps(accessOptions: Map[String, String]) = {
    val props = new Properties()
    accessOptions.foreach { option =>
      props.put(option._1, option._2)
    }
    props
  }

  def topicSaveOffsets(
    topicConfigName: String,
    accessOptions: Map[String, String],
    offsets: List[(Int, Long)]
  ): Unit = {
    cometOffsetsMode match {
      case Mode.STREAM =>
        val props: Properties = buildProps(accessOptions)
        val producer = new KafkaProducer[String, String](props)
        offsets.foreach { case (partition, offset) =>
          producer.send(
            new ProducerRecord[String, String](
              cometOffsetsConfig.topicName,
              s"$topicConfigName/$partition",
              s"$offset"
            )
          )
        }
        producer.close()
      case Mode.FILE =>
        cometOffsetsLock(topicConfigName).doExclusively() {
          val cometOffsetsPath = new Path(cometOffsetsConfig.topicName, topicConfigName)
          logger.info(s"Saving comet offsets to path $cometOffsetsPath")
          settings.storageHandler.write(
            YamlSerializer.serializeObject(offsets.map { case (partition, offset) =>
              partition.toString + "," + offset.toString
            }),
            cometOffsetsPath
          )
        }
      case _ =>
        throw new Exception("Should never happen")
    }
  }

  def adminTopicPartitions(topicName: String): List[TopicPartitionInfo] = {
    client
      .describeTopics(java.util.Collections.singleton(topicName))
      .allTopicNames()
      .get()
      .get(topicName)
      .partitions()
      .asScala
      .toList
  }

  private def topicCurrentOffsetsFromStream(topicConfigName: String): Option[List[(Int, Long)]] = {
    val props = new Properties()

    cometOffsetsConfig.allAccessOptions().foreach { option =>
      props.put(option._1, option._2)
    }
    val consumer = new KafkaConsumer[String, String](props)
    val partitions = extractPartitions(cometOffsetsConfig.topicName, consumer)
    consumer.assign(partitions.asJava)
    consumer.seekToBeginning(partitions.asJava)
    val offsets = mutable.Map.empty[String, String]
    var records = consumer.poll(Duration.ofMillis(100))
    while (records != null && !records.isEmpty) {
      records
        .records(cometOffsetsConfig.topicName)
        .asScala
        .foreach(r => offsets += r.key() -> r.value())
      records = consumer.poll(Duration.ofMillis(100))
    }

    // (topic/partition, offset)
    val res = offsets.keys.map { k =>
      val tab = k.split('/')
      (tab(0), tab(1), offsets(k))
    } groupBy { case (topic, _, _) => topic } mapValues (_.map { case (topic, partition, offset) =>
      (partition.toInt, offset.toLong)
    }.toList) get topicConfigName
    res

  }

  private def cometOffsetsLock(topicConfigName: String): FileLock = {
    val lockPath = new Path(settings.comet.lock.path, s"comet_offsets_$topicConfigName.lock")
    new FileLock(lockPath, settings.storageHandler)
  }

  private def topicCurrentOffsetsFromFile(topicConfigName: String): Option[List[(Int, Long)]] = {
    cometOffsetsLock(topicConfigName).doExclusively() {
      val cometOffsetsPath = new Path(cometOffsetsConfig.topicName, topicConfigName)
      if (settings.storageHandler.exists(cometOffsetsPath)) {
        logger.info(s"Loading comet offsets to path $cometOffsetsPath")
        val res = YamlSerializer.mapper
          .readValue(
            settings.storageHandler.read(cometOffsetsPath),
            classOf[List[String]]
          )
          .map { str =>
            val tab = str.split(',')
            (tab(0).toInt, tab(1).toLong)
          }
        Some(res)
      } else {
        logger.info(s"Cannot load comet offsets: $cometOffsetsPath file does not exist")
        None
      }
    }
  }

  def topicCurrentOffsets(topicConfigName: String): Option[List[(Int, Long)]] = {
    cometOffsetsMode match {
      case Mode.STREAM =>
        topicCurrentOffsetsFromStream(topicConfigName)
      case Mode.FILE =>
        topicCurrentOffsetsFromFile(topicConfigName)
      case _ =>
        throw new Exception("Should never happen")
    }
  }

  def offsetsAsJson(topicName: String, offsets: List[(Int, Long)]): Option[String] = {
    if (offsets.isEmpty)
      None
    else {
      val offsetsAsString = offsets.map { case (partition, partitionOffset) =>
        s""""$partition": $partitionOffset"""
      } mkString ","
      Some(s"""{"$topicName":{$offsetsAsString}}""")
    }

  }

  def consumeTopicBatch(
    topicConfigName: String,
    session: SparkSession,
    config: KafkaTopicConfig
  ): (DataFrame, List[(Int, Long)]) = {
    val EARLIEST_OFFSET = -2L
    val startOffsets =
      topicCurrentOffsets(topicConfigName)
        .getOrElse {
          val consumer =
            newConsumer(
              config.topicName,
              config.allAccessOptions()
            )
          val partitions = extractPartitions(config.topicName, consumer)
            .map(p => (p.partition(), EARLIEST_OFFSET))
          consumer.close()
          partitions
        }

    logger.whenInfoEnabled {
      startOffsets.foreach { case (partition, offsetStart) =>
        logger.info(s"$topicConfigName start-offset -> $partition:$offsetStart")
      }
    }

    val endOffsets =
      topicEndOffsets(
        config.topicName,
        config.allAccessOptions()
      )
    logger.whenInfoEnabled {
      endOffsets.foreach { case (partition, offsetEnd) =>
        logger.info(s"$topicConfigName end-offset -> $partition:$offsetEnd")
      }
    }

    // We do not use the topic Name but the config name to allow us to
    // consume differently the same topic
    val withOffsetsTopicOptions =
      config.allAccessOptions() ++ Seq(
        "startingOffsets" -> offsetsAsJson(config.topicName, startOffsets).getOrElse("earliest"),
        "endingOffsets"   -> offsetsAsJson(config.topicName, endOffsets).getOrElse("latest")
      )
    // TODO Loop based on maxRead need to be implemented here

    logger.info("withOffsetsTopicOptions:" + withOffsetsTopicOptions.toString())
    logger.info(
      "settings.comet.kafka.sparkServerOptions:" + settings.comet.kafka.sparkServerOptions
        .toString()
    )
    val reader = session.read.format("kafka")
    val df =
      reader
        .options(withOffsetsTopicOptions)
        .options(settings.comet.kafka.sparkServerOptions)
        .load()
        .selectExpr(config.fields: _*)

    logger.whenInfoEnabled {
      logger.info(df.schemaString())
    }
    (df, endOffsets)
  }

  def sinkToTopic(
    config: KafkaTopicConfig,
    df: DataFrame
  ): Unit = {
    df.printSchema()
    df.write
      .format("kafka")
      .options(config.allAccessOptions())
      .option("topic", config.topicName)
      .save()
  }
}

object KafkaClient {
  def consumeTopicStreaming(
    session: SparkSession,
    config: KafkaTopicConfig
  )(implicit settings: Settings): DataFrame = {
    val reader = session.readStream.format("kafka")
    val df =
      reader
        .options(config.allAccessOptions())
        .load()
        .selectExpr(config.fields: _*)
    df
  }

}
