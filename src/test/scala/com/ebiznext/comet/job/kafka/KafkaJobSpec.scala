package com.ebiznext.comet.job.kafka

import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}
import com.ebiznext.comet.TestHelper
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
      assert(topics.size() >= 0)

    }
  }
}
