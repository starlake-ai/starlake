package com.ebiznext.comet.utils.kafka

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.config.Settings.{KafkaConfig, KafkaTopicConfig}
import com.ebiznext.comet.schema.generator.YamlSerializer
import com.ebiznext.comet.schema.model.Mode
import com.ebiznext.comet.utils.FileLock
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.{TopicPartition, TopicPartitionInfo}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable

class KafkaClient(kafkaConfig: KafkaConfig)(implicit settings: Settings)
    extends StrictLogging
    with AutoCloseable {

  val cometOffsetsMode: Mode =
    settings.comet.kafka.cometOffsetsMode.map(Mode.fromString).getOrElse(Mode.STREAM)
  val serverOptions: Map[String, String] = kafkaConfig.serverOptions
  val cometOffsetsConfig: KafkaTopicConfig = kafkaConfig.topics("comet_offsets")

  val props = new Properties()
  serverOptions.foreach { case (k, v) =>
    props.put(k, v)
  }

  lazy val client: AdminClient = AdminClient.create(props)

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
    client.listTopics().names().get().asScala.toSet.foreach(println)
    if (found) {
      client.deleteTopics(List(topicName).asJavaCollection)
    }
  }

  def createTopicIfNotPresent(topic: NewTopic, conf: Map[String, String]): Unit = {
    val found = client.listTopics().names().get().contains(topic.name)
    if (!found) {
      client.createTopics(java.util.Collections.singleton(topic.configs(conf.asJava))).all().get()
    }
  }

  def topicPartitions(topicName: String): List[TopicPartitionInfo] = {
    client
      .describeTopics(java.util.Collections.singleton(topicName))
      .all()
      .get()
      .get(topicName)
      .partitions()
      .asScala
      .toList
  }

  def topicEndOffsets(topicName: String, accessOptions: Map[String, String]): List[(Int, Long)] = {
    try {
      val props: Properties = buildProps(accessOptions)
      val consumer = new KafkaConsumer[String, String](props)
      logger.whenInfoEnabled {
        import scala.collection.JavaConverters._
        logger.info(s"access options for topic $topicName ==>")
        props.asScala.foreach { case (k, v) =>
          logger.info(s"\t$k=$v")
        }
      }
      val partitions = consumer
        .partitionsFor(topicName)
        .asScala
        .map(info => new TopicPartition(topicName, info.partition()))
        .toList
      consumer.assign(partitions.asJava)
      consumer.seekToEnd(partitions.asJava)
      partitions.map(p => (p.partition(), consumer.position(p)))
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        throw e
    }
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

  private def topicCurrentOffsetsFromStream(topicConfigName: String): Option[List[(Int, Long)]] = {
    val props = new Properties()
    cometOffsetsConfig.accessOptions.foreach { option =>
      props.put(option._1, option._2)
    }
    val consumer = new KafkaConsumer[String, String](props)
    val partitions =
      topicPartitions(cometOffsetsConfig.topicName).map(info =>
        new TopicPartition(cometOffsetsConfig.topicName, info.partition())
      )
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
          topicPartitions(config.topicName)
            .map(p => (p.partition(), EARLIEST_OFFSET))
        }

    logger.whenInfoEnabled {
      startOffsets.foreach { case (partition, offsetStart) =>
        logger.info(s"$topicConfigName start-offset -> $partition:$offsetStart")
      }
    }

    val endOffsets =
      topicEndOffsets(config.topicName, config.accessOptions)
    logger.whenInfoEnabled {
      endOffsets.foreach { case (partition, offsetEnd) =>
        logger.info(s"$topicConfigName end-offset -> $partition:$offsetEnd")
      }
    }

    // We do not use the topic Name but the config name to allow us to
    // consume differently the same topic
    val withOffsetsTopicOptions = config.accessOptions ++ Seq(
      "startingOffsets" -> offsetsAsJson(config.topicName, startOffsets).getOrElse("earliest"),
      "endingOffsets"   -> offsetsAsJson(config.topicName, endOffsets).getOrElse("latest")
    )
    // TODO Loop based on maxRead need to be implemented here

    val reader = session.read.format("kafka")
    val df =
      withOffsetsTopicOptions
        .foldLeft(reader) { case (reader, (k, v)) => reader.option(k, v) }
        .load()
        .selectExpr(config.fields.map(x => s"CAST($x)"): _*)

    logger.whenInfoEnabled(df.printSchema())
    (df, endOffsets)
  }

  def consumeTopicStreaming(
    session: SparkSession,
    config: KafkaTopicConfig
  ): DataFrame = {
    val reader = session.readStream.format("kafka")
    val df =
      config.accessOptions
        .foldLeft(reader) { case (reader, (k, v)) => reader.option(k, v) }
        .load()
        .selectExpr(config.fields.map(x => s"CAST($x)"): _*)
    df
  }

  def sinkToTopic(
    config: KafkaTopicConfig,
    df: DataFrame
  ): Unit = {
    val writer = df.selectExpr(config.fields.map(x => s"CAST($x)"): _*).write.format("kafka")

    config.accessOptions
      .foldLeft(writer) { case (writer, (k, v)) => writer.option(k, v) }
      .option("topic", config.topicName)
      .save()
  }
}
