package ai.starlake.job.kafka

import ai.starlake.TestHelper
import ai.starlake.job.sink.kafka.{KafkaJob, KafkaJobConfig}
import ai.starlake.utils.kafka.KafkaClient
import better.files.File
import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.{ElasticsearchContainer, KafkaContainer}
import com.typesafe.config.ConfigFactory
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SaveMode

import java.util.{Properties, UUID}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

class KafkaJobSpec extends TestHelper {
  type Containers = KafkaContainer and ElasticsearchContainer

  override def afterAll(): Unit = {
    super.afterAll()
    kafkaContainer.stop()
    esContainer.stop()
  }

  val esPort =
    s"${esContainer.httpHostAddress.substring(esContainer.httpHostAddress.lastIndexOf(':') + 1)}"

  def kafkaConfig(cometOffsetsMode: String, cometOffsetTopicName: String) = ConfigFactory
    .parseString(s"""
         |kafka {
         |  serverOptions = {
         |      "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |  }
         |  cometOffsetsMode = "$cometOffsetsMode"
         |  topics {
         |    "test_offload": {
         |      topicName: "test_offload"
         |      maxRead = 0
         |      fields = ["cast(key as STRING)", "cast(value as STRING)"]
         |      writeFormat = "parquet"
         |      accessOptions = {
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
         |      topicName: "test_offload_kafka_to_kafka"
         |      maxRead = 0
         |      fields = ["cast(key as STRING)", "cast(value as STRING)"]
         |      accessOptions = {
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
         |    "test_offload_http_to_kafka": {
         |      topicName: "test_offload_http_to_kafka"
         |      maxRead = 0
         |      fields = ["cast(value as STRING)"]
         |      accessOptions = {
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
         |    "test_offload_kafka_to_http": {
         |      topicName: "test_offload_kafka_to_http"
         |      maxRead = 0
         |      fields = ["cast(key as STRING)", "cast(value as STRING)"]
         |      writeFormat = "parquet"
         |      accessOptions = {
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
         |      topicName: "test_load"
         |      maxRead = 0
         |      fields = ["cast(key as STRING)", "cast(value as STRING)"]
         |      writeFormat = "parquet"
         |      accessOptions = {
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
         |      topicName: "kafka_to_es"
         |      maxRead = 0
         |      fields = ["cast(key as STRING)", "cast(value as STRING)"]
         |      writeFormat = "parquet"
         |      accessOptions = {
         |        "kafka.bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "key.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "subscribe": "kafka_to_es"
         |      }
         |    },
         |    "topic_sink_config": {
         |      topicName: "topic_sink"
         |      maxRead = 0
         |      fields = ["cast(key as STRING)", "cast(value as STRING)"]
         |      writeFormat = "parquet"
         |      accessOptions = {
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
         |      topicName: "stream_kafka_to_es"
         |      maxRead = 0
         |      fields = ["cast(key as STRING)", "cast(value as STRING)"]
         |      writeFormat = "parquet"
         |      accessOptions = {
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
         |      topicName: "stream_kafka_to_table"
         |      maxRead = 0
         |      fields = ["cast(key as STRING)", "cast(value as STRING)"]
         |      writeFormat = "parquet"
         |      accessOptions = {
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
         |      topicName: "$cometOffsetTopicName"
         |      maxRead = 0
         |      partitions = 1
         |      replication-factor = 1
         |      writeFormat = "parquet"
         |      createOptions {
         |        "cleanup.policy": "compact"
         |      }
         |      accessOptions = {
         |        "kafka.bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "auto.offset.reset": "earliest"
         |        "auto.commit.enable": "false"
         |        "consumer.timeout.ms": "10"
         |        "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
         |        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "subscribe": "$cometOffsetTopicName"
         |      }
         |    }
         |  }
         |}
         |connections.elasticsearch {
         |  type="elasticsearch"
         |  format = "elasticsearch"
         |  mode = "Append"
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
          File("/tmp/comet_offsets").delete(swallowIOExceptions = true)
        val properties = new Properties()
        properties.put("bootstrap.servers", kafkaContainer.bootstrapServers)
        properties.put("group.id", "consumer-test")
        properties.put("key.deserializer", classOf[StringDeserializer])
        properties.put("value.deserializer", classOf[StringDeserializer])

        val kafkaConsumer = new KafkaConsumer[String, String](properties)
        kafkaConsumer.listTopics()
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Create topic comet_offsets topic" should "succeed" in {
        val kafkaClient = new KafkaClient(settings.appConfig.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete(swallowIOExceptions = true)
        kafkaClient.deleteTopic("test_offload")
        val topicProperties = Map("cleanup.policy" -> "compact")
        Thread.sleep(1000) // wait for topic to be deleted
        kafkaClient.createTopicIfNotPresent(
          new NewTopic("comet_offsets", 1, 1.toShort),
          topicProperties
        )
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Get comet_offsets partitions " should "return 1" in {
        val kafkaClient = new KafkaClient(settings.appConfig.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete(swallowIOExceptions = true)
        kafkaClient.deleteTopic("test_offload")
        val topicProperties = Map("cleanup.policy" -> "compact")
        Thread.sleep(1000) // wait for topic to be deleted
        kafkaClient.createTopicIfNotPresent(
          new NewTopic("comet_offsets", 1, 1.toShort),
          topicProperties
        )
        val partitions = kafkaClient.adminTopicPartitions("comet_offsets")
        partitions should have size 1
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Access comet_offsets end offsets" should "work" in {
        val kafkaClient = new KafkaClient(settings.appConfig.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete(swallowIOExceptions = true)
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
        file.delete(swallowIOExceptions = true)
        file.overwrite(elems.mkString("\n"))
        file
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Offload messages from Kafka" should "work" in {
        val kafkaClient = new KafkaClient(settings.appConfig.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete(swallowIOExceptions = true)
        kafkaClient.deleteTopic("test_offload")
        Thread.sleep(1000) // wait for topic to be deleted
        kafkaClient.createTopicIfNotPresent(new NewTopic("test_offload", 1, 1.toShort), Map.empty)
        val file = createTempJsonDataFile(5000)
        val kafkaJob =
          new KafkaJob(
            KafkaJobConfig(
              path = Some(file.pathAsString),
              format = "json",
              writeTopicConfigName = Some("test_offload"),
              writeMode = SaveMode.Append.toString,
              writeFormat = "kafka"
            ),
            schemaHandler = settings.schemaHandler()
          )
        kafkaJob.run()
        val kafkaJob2 =
          new KafkaJob(
            KafkaJobConfig(
              topicConfigName = Some("test_offload"),
              format = "kafka",
              writeMode = SaveMode.Overwrite.toString,
              writeFormat = "json",
              writePath = Some("/tmp/json2.json"),
              coalesce = Some(1)
            ),
            schemaHandler = settings.schemaHandler()
          )
        kafkaJob2.run() match {
          case Success(_) =>
            sparkSession.read.json("/tmp/json2.json").count() shouldBe 5000
          case Failure(e) => throw e
        }
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Load data from file to Kafka" should "work" in {
        val kafkaClient = new KafkaClient(settings.appConfig.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete(swallowIOExceptions = true)
        kafkaClient.deleteTopic("test_offload")
        val file = createTempJsonDataFile(10000)
        kafkaClient.deleteTopic("test_load")
        Thread.sleep(1000) // wait for topic to be deleted
        kafkaClient.createTopicIfNotPresent(new NewTopic("test_load", 1, 1.toShort), Map.empty)
        val kafkaJob =
          new KafkaJob(
            KafkaJobConfig(
              format = "json",
              path = Some(file.pathAsString),
              writeTopicConfigName = Some("test_load"),
              writeMode = SaveMode.Append.toString
            ),
            schemaHandler = settings.schemaHandler()
          )
        kafkaJob.run()
        val offsets =
          kafkaClient.topicEndOffsets(
            "test_load",
            settings.appConfig.kafka
              .topics("test_load")
              .allAccessOptions()
          )
        offsets should contain theSameElementsAs List((0, 10000))
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Offload from Kafka to Elasticsearch" should "work" in {
        val kafkaClient = new KafkaClient(settings.appConfig.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete(swallowIOExceptions = true)
        kafkaClient.deleteTopic("test_offload")
        val file = createTempJsonDataFile(100)
        kafkaClient.deleteTopic("kafka_to_es")
        Thread.sleep(1000) // wait for topic to be deleted
        kafkaClient.createTopicIfNotPresent(new NewTopic("kafka_to_es", 1, 1.toShort), Map.empty)
        val kafkaJob =
          new KafkaJob(
            KafkaJobConfig(
              format = "json",
              path = Some(file.pathAsString),
              writeTopicConfigName = Some("kafka_to_es"),
              writeMode = SaveMode.Overwrite.toString
            ),
            schemaHandler = settings.schemaHandler()
          )
        kafkaJob.run()
        val offsets =
          kafkaClient.topicEndOffsets(
            "kafka_to_es",
            settings.appConfig.kafka
              .topics("kafka_to_es")
              .allAccessOptions()
          )
        offsets should contain theSameElementsAs List((0, 100))

        val kafkaJobToEs =
          new KafkaJob(
            KafkaJobConfig(
              topicConfigName = Some("kafka_to_es"),
              writeFormat = "org.elasticsearch.spark.sql",
              writeMode = SaveMode.Overwrite.toString,
              writePath = Some("test/_doc"),
              writeOptions = settings.appConfig.connectionOptions("elasticsearch")
            ),
            schemaHandler = settings.schemaHandler()
          )
        kafkaJobToEs.run()

        val countUri = s"http://${esContainer.httpHostAddress}/test/_count"
        val getRequest = new HttpGet(countUri)
        getRequest.setHeader("Content-Type", "application/json")
        val client = HttpClients.createDefault
        val response = client.execute(getRequest)

        response.getStatusLine.getStatusCode should be <= 299
        response.getStatusLine.getStatusCode should be >= 200
        EntityUtils.toString(response.getEntity()) contains "\"count\":100"
      }

      s"$cometOffsetsMode($cometOffsetTopicName) Stream from Kafka to Elasticsearch" should "work" in {
        val kafkaClient = new KafkaClient(settings.appConfig.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete(swallowIOExceptions = true)
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
              path = Some(file.pathAsString),
              format = "json",
              writeTopicConfigName = Some("stream_kafka_to_es"),
              writeMode = SaveMode.Append.toString
            ),
            schemaHandler = settings.schemaHandler()
          )
        kafkaJob.run()

        val kafkaJobToEs =
          new KafkaJob(
            KafkaJobConfig(
              streaming = true,
              topicConfigName = Some("stream_kafka_to_es"),
              writeFormat = "org.elasticsearch.spark.sql",
              writeMode = SaveMode.Overwrite.toString,
              writePath = Some("test/_doc"),
              writeOptions = settings.appConfig.connectionOptions("elasticsearch")
            ),
            schemaHandler = settings.schemaHandler()
          )
        kafkaJobToEs.run()

        val countUri = s"http://${esContainer.httpHostAddress}/test/_count"
        val getRequest = new HttpGet(countUri)
        getRequest.setHeader("Content-Type", "application/json")
        val client = HttpClients.createDefault
        val response = client.execute(getRequest)
        response.getStatusLine.getStatusCode should be <= 299
        response.getStatusLine.getStatusCode should be >= 200
        EntityUtils.toString(response.getEntity()) contains "\"count\":100"

      }
      /*
      s"$cometOffsetsMode($cometOffsetTopicName) Stream from HTTP to Kafka" should "succeed" in {
        val kafkaClient = new KafkaClient(settings.appConfig.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete(swallowIOExceptions = true)
        kafkaClient.deleteTopic("test_offload_http_to_kafka")
        Thread.sleep(1000) // wait for topic to be deleted
        kafkaClient.createTopicIfNotPresent(
          new NewTopic("test_offload_http_to_kafka", 1, 1.toShort),
          Map.empty
        )
        val kafkaJob =
          new KafkaJob(
            KafkaJobConfig(
              streaming = true,
              topicConfigName = Some("test_offload_http_to_kafka"),
              format = "starlake-http-source",
              writeMode = SaveMode.Append.toString,
              options = Map(
                "port" -> "9999",
                "name" -> "test_offload_http_to_kafka"
              ),
              path = "topic_sink",
              writeFormat = "kafka",
              streamingTrigger = Some("ProcessingTime"),
              streamingTriggerOption = "10 millisecond",
              writeOptions = Map(
                "topic"                   -> "topic_sink",
                "kafka.bootstrap.servers" -> s"${kafkaContainer.bootstrapServers}"
              )
            )
          )
        kafkaJob.run() match {
          case Success(_) =>
            val offsets =
              kafkaClient.topicEndOffsets(
                "topic_sink",
                settings.comet.kafka
                  .topics("topic_sink_config")
                  .allAccessOptions()
              )
            offsets should contain theSameElementsAs List((0, 5000))
          case Failure(e) => throw e
        }
      }
       */
      s"$cometOffsetsMode($cometOffsetTopicName) Stream from Kafka to Kafka" should "succeed" in {
        val kafkaClient = new KafkaClient(settings.appConfig.kafka)
        if (cometOffsetsMode == "FILE")
          File("/tmp/comet_offsets").delete(swallowIOExceptions = true)
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
              format = "json",
              path = Some(file.pathAsString),
              writeTopicConfigName = Some("test_offload_kafka_to_kafka"),
              writeMode = SaveMode.Append.toString
            ),
            schemaHandler = settings.schemaHandler()
          )
        kafkaJob.run()
        val kafkaJob2 =
          new KafkaJob(
            KafkaJobConfig(
              streaming = true,
              topicConfigName = Some("test_offload_kafka_to_kafka"),
              format = "json",
              writeMode = SaveMode.Append.toString,
              writeFormat = "kafka",
              writeOptions = Map(
                "topic"                   -> "topic_sink",
                "kafka.bootstrap.servers" -> s"${kafkaContainer.bootstrapServers}"
              )
            ),
            schemaHandler = settings.schemaHandler()
          )
        kafkaJob2.run() match {
          case Success(_) =>
            val offsets =
              kafkaClient.topicEndOffsets(
                "topic_sink",
                settings.appConfig.kafka
                  .topics("topic_sink_config")
                  .allAccessOptions()
              )
            offsets should contain theSameElementsAs List((0, 5000))
          case Failure(e) => throw e
        }
      }
    }

  if (sys.env.get("SL_KAFKA_TEST_DISABLE").isEmpty) {
    kafkaTests("FILE", "/tmp/comet_offsets")
    kafkaTests("STREAM", "comet_offsets")
  }
}
