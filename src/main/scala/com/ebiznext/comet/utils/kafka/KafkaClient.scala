package com.ebiznext.comet.utils.kafka

import com.ebiznext.comet.config.Settings.{KafkaConfig, KafkaTopicOptions}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.{TopicPartition, TopicPartitionInfo}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Duration
import java.util.Properties
import scala.collection.mutable
import collection.JavaConverters._

class KafkaClient(kafkaConfig: KafkaConfig) extends StrictLogging with AutoCloseable {

  val serverOptions: Map[String, String] = kafkaConfig.serverOptions
  val cometOffsetsConfig: KafkaTopicOptions = kafkaConfig.topics("comet_offsets")

  val props = new Properties()
  serverOptions.foreach { option =>
    props.put(option._1, option._2)
  }

  val client: AdminClient = AdminClient.create(props)

  createTopicIfNotPresent(
    new NewTopic(
      "comet_offsets",
      cometOffsetsConfig.partitions,
      cometOffsetsConfig.replicationFactor
    ),
    Map("cleanup.policy" -> "compact")
  )

  def close(): Unit = client.close()

  def deleteTopic(topicName: String): Unit = {
    val found = client.listTopics().names().get().contains(topicName)
    client.listTopics().names().get().asScala.toSet.foreach(println)
    if (found) {
      client.deleteTopics(List(topicName).asJavaCollection)
    }
  }

  def createTopicIfNotPresent(topic: NewTopic, conf: Map[String, String]): Any = {
    val found = client.listTopics().names().get().contains(topic.name)
    if (!found) {
      topic.configs(conf.asJava)
      client.createTopics(java.util.Collections.singleton(topic)).all().get()
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
    val props: Properties = buildProps(accessOptions)
    val consumer = new KafkaConsumer[String, String](props)
    val partitions = consumer
      .partitionsFor(topicName)
      .asScala
      .map(info => new TopicPartition(topicName, info.partition()))
      .toList
    consumer.assign(partitions.asJava)
    consumer.seekToEnd(partitions.asJava)
    partitions.map(p => (p.partition(), consumer.position(p)))
  }

  private def buildProps(accessOptions: Map[String, String]) = {
    val props = new Properties()
    accessOptions.foreach { option =>
      props.put(option._1, option._2)
    }
    props
  }

  def topicSaveOffsets(
    topicName: String,
    accessOptions: Map[String, String],
    offsets: List[(Int, Long)]
  ): Unit = {
    val props: Properties = buildProps(accessOptions)
    val producer = new KafkaProducer[String, String](props)
    offsets.foreach { case (partition, offset) =>
      producer.send(
        new ProducerRecord[String, String]("comet_offsets", s"$topicName/$partition", s"$offset")
      )
    }
    producer.close()
  }

  def topicCurrentOffsets(topicName: String): Option[List[(Int, Long)]] = {
    val props = new Properties()
    cometOffsetsConfig.accessOptions.foreach { option =>
      props.put(option._1, option._2)
    }
    val consumer = new KafkaConsumer[String, String](props)
    val partitions =
      topicPartitions("comet_offsets").map(info =>
        new TopicPartition("comet_offsets", info.partition())
      )
    consumer.assign(partitions.asJava)
    consumer.seekToBeginning(partitions.asJava)
    val offsets = mutable.Map.empty[String, String]
    var records = consumer.poll(Duration.ofMillis(100))
    while (records != null && !records.isEmpty) {
      records.records("comet_offsets").asScala.foreach(r => offsets += r.key() -> r.value())
      records = consumer.poll(Duration.ofMillis(100))
    }

    // (topic/partition, offset)
    val res = offsets.keys.map { k =>
      val tab = k.split('/')
      (tab(0), tab(1), offsets(k))
    } groupBy (_._1) mapValues (_.map(it => (it._2.toInt, it._3.toLong)).toList) get topicName
    res
  }

  def offsetsAsJson(topicConfigName: String, offsets: List[(Int, Long)]): Option[String] = {
    if (offsets.isEmpty)
      None
    else {

      val offsetsAsString = offsets.map { offset =>
        s""""${offset._1}": ${offset._2}"""
      } mkString ","
      Some(s"""{"$topicConfigName":{$offsetsAsString}}""")
    }

  }

  def consumeTopicBatch(
    topicConfigName: String,
    session: SparkSession,
    config: KafkaTopicOptions
  ): (DataFrame, List[(Int, Long)]) = {
    val EARLIEST_OFFSET = -2L
    val startOffsets =
      topicCurrentOffsets(config.name.getOrElse(topicConfigName))
        .getOrElse {
          topicPartitions(config.name.getOrElse(topicConfigName)).map(p =>
            (p.partition(), EARLIEST_OFFSET)
          )
        }
    val endOffsets = topicEndOffsets(config.name.getOrElse(topicConfigName), config.accessOptions)

    // We do not use the topic Name but the config name to allow us to
    // consume differently the same topic
    val withOffsetsTopicOptions = config.accessOptions ++ Seq(
      "startingOffsets" -> offsetsAsJson(topicConfigName, startOffsets).getOrElse("earliest"),
      "endingOffsets"   -> offsetsAsJson(topicConfigName, endOffsets).getOrElse("latest")
    )
    // TODO Loop based on maxRead need to be implemented here

    val reader = session.read.format("kafka")
    val df =
      withOffsetsTopicOptions
        .foldLeft(reader)((reader, option) => reader.option(option._1, option._2))
        .load()
        .selectExpr(config.fields.map(x => s"CAST($x)"): _*)
    df.printSchema()
    (df, endOffsets)
  }

  def consumeTopicStreaming(
    topicConfigName: String,
    session: SparkSession,
    config: KafkaTopicOptions
  ): DataFrame = {
    val reader = session.readStream.format("kafka")
    val df =
      config.accessOptions
        .foldLeft(reader)((reader, option) => reader.option(option._1, option._2))
        .load()
        .selectExpr(config.fields.map(x => s"CAST($x)"): _*)
    df
  }

  def sinkToTopic(
    topicConfigName: String,
    config: KafkaTopicOptions,
    df: DataFrame
  ): Unit = {
    val writer = df.selectExpr(config.fields.map(x => s"CAST($x)"): _*).write.format("kafka")

    config.accessOptions
      .foldLeft(writer)((writer, option) => writer.option(option._1, option._2))
      .option("topic", config.name.getOrElse(topicConfigName))
      .save()
  }
}
