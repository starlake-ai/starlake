package com.ebiznext.comet.job.kafka

import better.files.File
import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.{ElasticsearchContainer, KafkaContainer}
import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.job.index.kafkaload.{KafkaJob, KafkaJobConfig}
import com.ebiznext.comet.utils.kafka.KafkaClient
import com.softwaremill.sttp.{HttpURLConnectionBackend, _}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SaveMode

import java.util.{Properties, UUID}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

class KafkaJobSpec extends TestHelper {
  type Containers = KafkaContainer and ElasticsearchContainer

  //https://scala.monster/testcontainers/
  // We need to start iit manually because we need to access the HTTP mapped port
  // in the configuration below before any test get executed.
  val kafkaContainer: KafkaContainer = KafkaContainer.Def().start()
  val esContainer: ElasticsearchContainer = ElasticsearchContainer.Def().start()

  override def afterAll(): Unit = {
    kafkaContainer.stop()
    esContainer.stop()
    super.afterAll()
  }

  val esPort =
    s"${esContainer.httpHostAddress.substring(esContainer.httpHostAddress.lastIndexOf(':') + 1)}"

  def kafkaConfig(cometOffsetsMode: String, cometOffsetTopicName: String) = ConfigFactory
    .parseString(s"""
         |kafka {
         |  server-options = {
         |      "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |  }
         |  comet-offsets-mode = "$cometOffsetsMode"
         |  topics {
         |    "test_offload": {
         |      topic-name: "test_offload"
         |      max-read = 0
         |      fields = ["key as STRING", "value as STRING"]
         |      write-format = "parquet"
         |      access-options = {
         |        "kafka.bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "key.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "subscribe": "test_offload"
         |        "startingOffsets": "earliest"
         |      }
         |    },
         |    "test_offload_kafka_to_kafka": {
         |      topic-name: "test_offload_kafka_to_kafka"
         |      max-read = 0
         |      fields = ["key as STRING", "value as STRING"]
         |      write-format = "parquet"
         |      access-options = {
         |        "kafka.bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "key.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "subscribe": "test_offload_kafka_to_kafka"
         |        "startingOffsets": "earliest"
         |      }
         |    },
         |    "test_load": {
         |      topic-name: "test_load"
         |      max-read = 0
         |      fields = ["key as STRING", "value as STRING"]
         |      write-format = "parquet"
         |      access-options = {
         |        "kafka.bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "key.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "subscribe": "test_load"
         |      }
         |    },
         |    "kafka_to_es": {
         |      topic-name: "kafka_to_es"
         |      max-read = 0
         |      fields = ["key as STRING", "value as STRING"]
         |      write-format = "parquet"
         |      access-options = {
         |        "kafka.bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "key.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "subscribe": "test_load"
         |      }
         |    },
         |    "topic_sink_config": {
         |      topic-name: "topic_sink"
         |      max-read = 0
         |      fields = ["key as STRING", "value as STRING"]
         |      write-format = "parquet"
         |      access-options = {
         |        "kafka.bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "key.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "subscribe": "test_stream"
         |      }
         |    },
         |    "stream_kafka_to_es": {
         |      topic-name: "stream_kafka_to_es"
         |      max-read = 0
         |      fields = ["key as STRING", "value as STRING"]
         |      write-format = "parquet"
         |      access-options = {
         |        "kafka.bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "key.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "subscribe": "test_load"
         |      }
         |    },
         |    "stream_kafka_to_table": {
         |      topic-name: "stream_kafka_to_table"
         |      max-read = 0
         |      fields = ["key as STRING", "value as STRING"]
         |      write-format = "parquet"
         |      access-options = {
         |        "kafka.bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "key.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "subscribe": "test_load"
         |      }
         |    },
         |    "comet_offsets": {
         |      topic-name: "$cometOffsetTopicName"
         |      max-read = 0
         |      partitions = 1
         |      replication-factor = 1
         |      write-format = "parquet"
         |      create-potions {
         |        "cleanup.policy": "compact"
         |      }
         |      access-options = {
         |        "kafka.bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "auto.offset.reset": "earliest"
         |        "auto.commit.enable": "false"
         |        "consumer.timeout.ms": "10"
         |        "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
         |        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "subscribe": "comet_offsets"
         |      }
         |    }
         |  }
         |}
         |elasticsearch {
         |  active = true
         |  options = {
         |    "es.nodes.wan.only": "true"
         |    "es.index.auto.create": "true"
         |    "es.nodes": "localhost"
         |    "es.port": "$esPort"
         |    #  net.http.auth.user = ""
         |    #  net.http.auth.pass = ""
         |
         |    "es.net.ssl": "false",
         |    "es.net.ssl.cert.allow.self.signed": "false"
         |
         |    "es.batch.size.entries": "1000"
         |    "es.batch.size.bytes": "1mb"
         |    "es.batch.write.retry.count": "3"
         |    "es.batch.write.retry.wait": "10s"
         |  }
         |}
         |""".stripMargin)
    .withFallback(super.testConfiguration)

  def kafkaTests(cometOffsetsMode: String, cometOffsetTopicName: String): WithSettings =
    new WithSettings(kafkaConfig(cometOffsetsMode, cometOffsetTopicName)) {
      s"$cometOffsetsMode($cometOffsetTopicName) runs with embedded kafka" should "work" in {
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete()
        val properties = new Properties()
        properties.put("bootstrap.servers", kafkaContainer.bootstrapServers)
        properties.put("group.id", "consumer-test")
        properties.put("key.deserializer", classOf[StringDeserializer])
        properties.put("value.deserializer", classOf[StringDeserializer])

        val kafkaConsumer = new KafkaConsumer[String, String](properties)
        kafkaConsumer.listTopics()
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Create topic comet_offsets topic" should "succeed" in {
        val kafkaClient = new KafkaClient(settings.comet.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete()
        kafkaClient.deleteTopic("test_offload")
        val topicProperties = Map("cleanup.policy" -> "compact")
        Thread.sleep(1000) // wait for topic to be deleted
        kafkaClient.createTopicIfNotPresent(
          new NewTopic("comet_offsets", 1, 1.toShort),
          topicProperties
        )
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Get comet_offsets partitions " should "return 1" in {
        val kafkaClient = new KafkaClient(settings.comet.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete()
        kafkaClient.deleteTopic("test_offload")
        val topicProperties = Map("cleanup.policy" -> "compact")
        Thread.sleep(1000) // wait for topic to be deleted
        kafkaClient.createTopicIfNotPresent(
          new NewTopic("comet_offsets", 1, 1.toShort),
          topicProperties
        )
        val partitions = kafkaClient.topicPartitions("comet_offsets")
        partitions should have size 1
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Access comet_offsets end offsets" should "work" in {
        val kafkaClient = new KafkaClient(settings.comet.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete()
        kafkaClient.deleteTopic("test_offload")
        val topicProperties = Map("cleanup.policy" -> "compact")
        Thread.sleep(1000) // wait for topic to be deleted
        kafkaClient.createTopicIfNotPresent(
          new NewTopic("comet_offsets", 1, 1.toShort),
          topicProperties
        )
        val properties = Map(
          "bootstrap.servers"  -> kafkaContainer.bootstrapServers,
          "key.deserializer"   -> "org.apache.kafka.common.serialization.StringDeserializer",
          "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )
        val offsets = kafkaClient.topicEndOffsets("comet_offsets", properties)
        offsets should contain theSameElementsAs List((0, 0))
      }

      private def createTempJsonDataFile(count: Int): File = {
        val elems = ListBuffer[String]()
        for (i <- 1 to count) {
          elems += s"""{"key": "key-$i","value": "${UUID.randomUUID().toString}-$i"}"""
        }
        val file = File("/tmp/json.json")
        file.delete()
        file.overwrite(elems.mkString("\n"))
        file
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Offload messages from Kafka" should "work" in {
        val kafkaClient = new KafkaClient(settings.comet.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete()
        kafkaClient.deleteTopic("test_offload")
        Thread.sleep(1000) // wait for topic to be deleted
        kafkaClient.createTopicIfNotPresent(new NewTopic("test_offload", 1, 1.toShort), Map.empty)
        val file = createTempJsonDataFile(5000)
        val kafkaJob =
          new KafkaJob(
            KafkaJobConfig(
              "test_offload",
              "json",
              SaveMode.Overwrite,
              file.pathAsString,
              offload = false
            )
          )
        kafkaJob.run()
        val kafkaJob2 =
          new KafkaJob(
            KafkaJobConfig(
              "test_offload",
              "json",
              SaveMode.Overwrite,
              "/tmp/json2.json",
              coalesce = Some(1)
            )
          )
        kafkaJob2.run() match {
          case Success(_) =>
            sparkSession.read.json("/tmp/json2.json").count() shouldBe 5000
          case Failure(e) => throw e
        }
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Load data from file to Kafka" should "work" in {
        val kafkaClient = new KafkaClient(settings.comet.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete()
        kafkaClient.deleteTopic("test_offload")
        val file = createTempJsonDataFile(10000)
        kafkaClient.deleteTopic("test_load")
        Thread.sleep(1000) // wait for topic to be deleted
        kafkaClient.createTopicIfNotPresent(new NewTopic("test_load", 1, 1.toShort), Map.empty)
        val kafkaJob =
          new KafkaJob(
            KafkaJobConfig(
              "test_load",
              "json",
              SaveMode.Overwrite,
              file.pathAsString,
              offload = false
            )
          )
        kafkaJob.run()
        val offsets =
          kafkaClient.topicEndOffsets(
            "test_load",
            settings.comet.kafka.topics("test_load").accessOptions
          )
        offsets should contain theSameElementsAs List((0, 10000))
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Offload from Kafka to Elasticsearch" should "work" in {
        if (settings.comet.isElasticsearchSupported()) {
          val kafkaClient = new KafkaClient(settings.comet.kafka)
          if (cometOffsetsMode == "FILE")
            File("/tmp/comet_offsets").delete()
          kafkaClient.deleteTopic("test_offload")
          val file = createTempJsonDataFile(100)
          kafkaClient.deleteTopic("kafka_to_es")
          Thread.sleep(1000) // wait for topic to be deleted
          kafkaClient.createTopicIfNotPresent(new NewTopic("kafka_to_es", 1, 1.toShort), Map.empty)
          val kafkaJob =
            new KafkaJob(
              KafkaJobConfig(
                "kafka_to_es",
                "json",
                SaveMode.Overwrite,
                file.pathAsString,
                offload = false
              )
            )
          kafkaJob.run()
          val offsets =
            kafkaClient.topicEndOffsets(
              "kafka_to_es",
              settings.comet.kafka.topics("kafka_to_es").accessOptions
            )
          offsets should contain theSameElementsAs List((0, 100))
          import scala.collection.JavaConverters._

          val kafkaJobToEs =
            new KafkaJob(
              KafkaJobConfig(
                "kafka_to_es",
                "org.elasticsearch.spark.sql",
                SaveMode.Overwrite,
                "test/_doc",
                writeOptions = settings.comet.elasticsearch.options.asScala.toMap
              )
            )
          kafkaJobToEs.run()
          implicit val backend = HttpURLConnectionBackend()
          val countUri = uri"http://${esContainer.httpHostAddress}/test/_count"
          val response = sttp.get(countUri).send()
          response.code should be <= 299
          response.code should be >= 200
          assert(response.body.isRight)
          response.body.right.toString() contains "\"count\":100"
        }
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Stream from Kafka to Elasticsearch" should "work" in {
        if (settings.comet.isElasticsearchSupported()) {
          val kafkaClient = new KafkaClient(settings.comet.kafka)
          if (cometOffsetsMode == "FILE")
            File("/tmp/comet_offsets").delete()
          kafkaClient.deleteTopic("test_offload")
          val file = createTempJsonDataFile(100)
          kafkaClient.deleteTopic("stream_kafka_to_es")
          Thread.sleep(1000) // wait for topic to be deleted
          kafkaClient.createTopicIfNotPresent(
            new NewTopic("stream_kafka_to_es", 1, 1.toShort),
            Map.empty
          )
          val kafkaJob =
            new KafkaJob(
              KafkaJobConfig(
                "stream_kafka_to_es",
                "json",
                SaveMode.Overwrite,
                file.pathAsString,
                offload = false
              )
            )
          kafkaJob.run()
          import scala.collection.JavaConverters._

          val kafkaJobToEs =
            new KafkaJob(
              KafkaJobConfig(
                "stream_kafka_to_es",
                "org.elasticsearch.spark.sql",
                SaveMode.Overwrite,
                "test/_doc",
                writeOptions = settings.comet.elasticsearch.options.asScala.toMap,
                streaming = true
              )
            )
          kafkaJobToEs.run()
          implicit val backend = HttpURLConnectionBackend()
          val countUri = uri"http://${esContainer.httpHostAddress}/test/_count"
          val response = sttp.get(countUri).send()
          response.code should be <= 299
          response.code should be >= 200
          assert(response.body.isRight)
          response.body.right.toString() contains "\"count\":100"
        }
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Stream from Kafka to Kafka" should "succeed" in {
        val kafkaClient = new KafkaClient(settings.comet.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete()
        kafkaClient.deleteTopic("test_offload_kafka_to_kafka")
        kafkaClient.deleteTopic("topic_sink")
        Thread.sleep(1000) // wait for topic to be deleted
        kafkaClient.createTopicIfNotPresent(
          new NewTopic("test_offload_kafka_to_kafka", 1, 1.toShort),
          Map.empty
        )
        val file = createTempJsonDataFile(5000)
        val kafkaJob =
          new KafkaJob(
            KafkaJobConfig(
              "test_offload_kafka_to_kafka",
              "json",
              SaveMode.Overwrite,
              file.pathAsString,
              offload = false
            )
          )
        kafkaJob.run()
        val kafkaJob2 =
          new KafkaJob(
            KafkaJobConfig(
              topicConfigName = "test_offload_kafka_to_kafka",
              format = "json",
              SaveMode.Append,
              path = "topic_sink",
              streaming = true,
              streamingWriteFormat = "kafka",
              writeOptions = Map(
                "topic"                   -> "topic_sink",
                "kafka.bootstrap.servers" -> s"${kafkaContainer.bootstrapServers}"
              )
            )
          )
        kafkaJob2.run() match {
          case Success(_) =>
            val offsets =
              kafkaClient.topicEndOffsets(
                "topic_sink",
                settings.comet.kafka.topics("topic_sink_config").accessOptions
              )
            offsets should contain theSameElementsAs List((0, 5000))
          case Failure(e) => throw e
        }
      }
    }

  kafkaTests("FILE", "/tmp/comet_offsets")
  kafkaTests("STREAM", "comet_offsets")
}
