package com.ebiznext.comet.utils.kafka

import com.ebiznext.comet.config.Settings.KafkaTopicOptions
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.{TopicPartition, TopicPartitionInfo}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Duration
import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class KafkaTopicUtils(serverOptions: Map[String, String]) extends StrictLogging {
  val props = new Properties()
  serverOptions.foreach { option =>
    props.put(option._1, option._2)
  }
  val client = AdminClient.create(props)

  def teardown(): Unit = client.close()

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
    val props = new Properties()
    accessOptions.foreach { option =>
      props.put(option._1, option._2)
    }
    val consumer = new KafkaConsumer[String, String](props)
    val partitions =
      topicPartitions(topicName).map(info => new TopicPartition(topicName, info.partition()))
    consumer.assign(partitions.asJava)
    consumer.seekToEnd(partitions.asJava)
    partitions.map(p => (p.partition(), consumer.position(p)))
  }

  def topicCurrentOffsets(
    topicName: String,
    accessOptions: Map[String, String]
  ): Option[List[(Int, Long)]] = {
    val props = new Properties()
    accessOptions.foreach { option =>
      props.put(option._1, option._2)
    }
    val consumer = new KafkaConsumer[String, String](props)
    val partitions =
      topicPartitions(topicName).map(info => new TopicPartition(topicName, info.partition()))
    consumer.assign(partitions.asJava)
    consumer.seekToBeginning(partitions.asJava)
    val offsets = mutable.Map.empty[String, String]
    var records = consumer.poll(Duration.ofMillis(1))
    while (records != null && !records.isEmpty) {
      records.records(topicName).asScala.foreach(r => offsets += r.key() -> r.value())
      records = consumer.poll(Duration.ofMillis(1))
    }
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

  def consumeTopic(
    topicConfigName: String,
    session: SparkSession,
    config: KafkaTopicOptions
  ): DataFrame = {
    val EARLIEST_OFFSET = -2L
    val reader = session.read
      .format("kafka")
    val startOffsets =
      topicCurrentOffsets(config.name.getOrElse(topicConfigName), config.accessOptions)
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

    val df =
      withOffsetsTopicOptions
        .foldLeft(reader)((reader, option) => reader.option(option._1, option._2))
        .load()
        .selectExpr(config.fields.map(x => s"CAST($x)"): _*)
    df.printSchema()
    logger.whenDebugEnabled {
      df.collect().foreach(r => logger.debug(r.toString()))
    }
    df
  }

  def sinkToTopic(
    topicConfigName: String,
    config: KafkaTopicOptions,
    df: DataFrame
  ): Unit = {
    val writer = df.selectExpr(config.fields.map(x => s"CAST($x)"): _*).write.format("kafka")

    config.accessOptions
      .foldLeft(writer)((reader, option) => reader.option(option._1, option._2))
      .option("topic", config.name.getOrElse(topicConfigName))
      .save()
  }
}
