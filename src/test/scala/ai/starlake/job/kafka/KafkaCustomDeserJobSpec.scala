package ai.starlake.job.kafka

import ai.starlake.TestHelper
import ai.starlake.job.sink.http.SinkTransformer
import ai.starlake.job.sink.kafka.{KafkaJob, KafkaJobConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Utils
import better.files.File
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.ConfigFactory
import org.apache.http.client.methods.HttpUriRequest
import org.apache.spark.sql.SaveMode

import scala.util.{Failure, Success}

object TestSinkTransformer extends SinkTransformer {
  val mapper: ObjectMapper = Utils.newJsonMapper()
  def requestUris(url: String, rows: Array[Seq[String]]): Seq[HttpUriRequest] = {
    rows.foreach { row =>
      val jsonValue = row(1)
      println("=========>" + jsonValue)
    }
    Nil
  }
}

class KafkaCustomDeserJobSpec extends TestHelper {
  def kafkaConfig(cometOffsetsMode: String, cometOffsetTopicName: String) = ConfigFactory
    .parseString(s"""
         |kafka {
         |  customDeserializer = "io.confluent.kafka.serializers.KafkaAvroDeserializer"
         |  #customDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"
         |  serverOptions = {
         |      "bootstrap.servers": "localhost:9092"
         |      "schema.registry.url": "http://localhost:8081"
         |  }
         |  cometOffsets-mode = "$cometOffsetsMode"
         |  topics {
         |    "avro_offload": {
         |      topicName: "users"
         |      maxRead = 0
         |      fields = ["key", "deserialize(value) as value", "timestamp"]
         |      writeFormat = "json"
         |      accessOptions = {
         |        "kafka.bootstrap.servers": "localhost:9092"
         |        "schema.registry.url": "http://localhost:8081"
         |        "bootstrap.servers": "localhost:9092"
         |        "subscribe": "users"
         |        "kafka.group.id": "mygroup"
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
         |        "kafka.bootstrap.servers": "localhost:9092"
         |        "schema.registry.url": "localhost:8081"
         |        "auto.offset.reset": "earliest"
         |        "auto.commit.enable": "false"
         |        "consumer.timeout.ms": "10"
         |        "bootstrap.servers": "localhost:9092"
         |        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
         |        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "subscribe": "$cometOffsetTopicName"
         |      }
         |    }
         |  }
         |}
         |""".stripMargin)
    .withFallback(super.testConfiguration)

  val cometOffsetsMode = "FILE"
  val cometOffsetTopicName = "/tmp/comet_offsets"

  new WithSettings(kafkaConfig(cometOffsetsMode, cometOffsetTopicName)) {
    s"$cometOffsetsMode($cometOffsetTopicName) Offload messages from Kafka" should "work" in {
      if (cometOffsetsMode == "FILE")
        File("/tmp/comet_offsets").delete(swallowIOExceptions = true)
      if (false) { // require explicit activation
        val kafkaJob2 =
          new KafkaJob(
            KafkaJobConfig(
              topicConfigName = Some("avro_offload"),
              streamingTrigger = None,
              writeFormat = "starlake-http",
              writeOptions = Map(
                "url"         -> "http://localhost:9000",
                "transformer" -> "ai.starlake.job.kafka.TestSinkTransformer"
              ),
              writeMode = SaveMode.Overwrite.toString,
              path = Some("/tmp/outdir.json"),
              streaming = true,
              coalesce = Some(1)
            ),
            schemaHandler = new SchemaHandler(settings.storageHandler())
          )
        kafkaJob2.run() match {
          case Success(_) =>
          case Failure(e) => throw e
        }
      }
    }
  }
}
