package com.ebiznext.comet.job.kafka

import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}
import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.utils.kafka.TopicAdmin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties

class KafkaJobSpec extends TestHelper with ForAllTestContainer {
  override val container = KafkaContainer()
  new WithSettings() {
    "runs with embedded kafka" should "work" in {
      val properties = new Properties()
      properties.put("bootstrap.servers", container.bootstrapServers)
      properties.put("group.id", "consumer-test")
      properties.put("key.deserializer", classOf[StringDeserializer])
      properties.put("value.deserializer", classOf[StringDeserializer])

      val kafkaConsumer = new KafkaConsumer[String, String](properties)
      val topics = kafkaConsumer.listTopics()
    }
    "Create topic comet_offets topic" should "succeed" in {
      val properties = Map("bootstrap.servers" -> container.bootstrapServers)
      val client = new TopicAdmin(properties)
      val topicProperties = Map("cleanup.policy" -> "compact")
      client.createTopicIfNotPresent(new NewTopic("comet_offsets", 1, 1.toShort), topicProperties)
    }

    "Get comet_offets partitions " should "return 1" in {
      val properties = Map("bootstrap.servers" -> container.bootstrapServers)
      val client = new TopicAdmin(properties)
      val topicProperties = Map("cleanup.policy" -> "compact")
      client.createTopicIfNotPresent(new NewTopic("comet_offsets", 1, 1.toShort), topicProperties)
      val partitions = client.topicPartitions("comet_offsets")
      partitions should have size 1
    }

    "Access comet_offets end offsets " should "work" in {
      val client = new TopicAdmin(Map("bootstrap.servers" -> container.bootstrapServers))
      val topicProperties = Map("cleanup.policy" -> "compact")
      client.createTopicIfNotPresent(new NewTopic("comet_offsets", 1, 1.toShort), topicProperties)
      val properties = Map(
        "bootstrap.servers"  -> container.bootstrapServers,
        "key.deserializer"   -> "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
      )

      val offsets = client.topicEndOffsets("comet_offsets", properties)
      offsets should contain theSameElementsAs List((0, 0))

    }
  }
}
