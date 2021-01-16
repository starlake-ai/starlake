package com.ebiznext.comet.utils.kafka

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.{TopicPartition, TopicPartitionInfo}

import java.util.Properties
import scala.jdk.CollectionConverters._

object Topic {
  val DefaultReplication: Short = 2
  val DefaultPartitions: Int = 1
}

final case class Topic(
  name: String,
  replication: Short = Topic.DefaultReplication,
  partitions: Int = Topic.DefaultPartitions,
  topicConfig: Properties = new Properties
)

class TopicAdmin(serverOptions: Map[String, String]) {
  val props = new Properties()
  serverOptions.foreach { option =>
    props.setProperty(option._1, option._2)
  }
  val client = AdminClient.create(props)

  def teardown(client: AdminClient): Unit = client.close()

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

  def topicEndOffsets(topicName: String): List[(Int, Long)] = {
    val props = new Properties()
    val consumer = new KafkaConsumer[String, String](props)
    val partitions =
      topicPartitions(topicName).map(info => new TopicPartition(topicName, info.partition()))
    consumer.assign(partitions.asJava)
    consumer.seekToEnd(partitions.asJava)
    partitions.map(p => (p.partition(), consumer.position(p)))
  }
}
