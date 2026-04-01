package ai.starlake.job.kafka

import ai.starlake.TestHelper
import ai.starlake.job.sink.kafka.{KafkaJob, KafkaJobConfig}
import ai.starlake.utils.kafka.KafkaClient
import better.files.File
import com.dimafeng.testcontainers.KafkaContainer
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.spark.sql.SaveMode

import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class KafkaBigQueryJobSpec extends TestHelper {
  type Containers = KafkaContainer

  // BQ env vars
  private val bqProject = sys.env.getOrElse("SL_BQ_TEST_PROJECT", "")
  private val bqDataset = sys.env.getOrElse("SL_BQ_TEST_DATASET", "starlake_kafka_test")
  private val bqBucket = sys.env.getOrElse("SL_BQ_TEST_BUCKET", "")

  // Unique prefix per run to avoid collisions
  private val runId = UUID.randomUUID().toString.replace("-", "").take(8)
  private val tablePrefix = s"kafka_bq_test_$runId"

  // Track tables created during tests for cleanup
  private val createdTables = scala.collection.mutable.ListBuffer[String]()

  override def afterAll(): Unit = {
    // Cleanup BQ tables
    if (bqProject.nonEmpty && bqBucket.nonEmpty) {
      Try {
        val bigquery =
          com.google.cloud.bigquery.BigQueryOptions.newBuilder
            .setProjectId(bqProject)
            .build()
            .getService
        createdTables.foreach { suffix =>
          val tableId =
            com.google.cloud.bigquery.TableId.of(bqProject, bqDataset, s"${tablePrefix}_$suffix")
          bigquery.delete(tableId)
          logger.info(s"Deleted BQ table: $tableId")
        }
      }
    }
    super.afterAll()
    kafkaContainer.stop()
  }

  private def bqTableRef(suffix: String): String = s"$bqProject:$bqDataset.${tablePrefix}_$suffix"
  private def bqTableDot(suffix: String): String =
    s"$bqProject.$bqDataset.${tablePrefix}_$suffix"

  private def seedBigQueryTable(suffix: String, count: Int): Unit = {
    createdTables += suffix
    val bigquery =
      com.google.cloud.bigquery.BigQueryOptions.newBuilder
        .setProjectId(bqProject)
        .build()
        .getService

    val tableName = s"${tablePrefix}_$suffix"
    val tableId = com.google.cloud.bigquery.TableId.of(bqProject, bqDataset, tableName)

    // Create table with schema
    val schema = com.google.cloud.bigquery.Schema.of(
      com.google.cloud.bigquery.Field.of("key", com.google.cloud.bigquery.StandardSQLTypeName.STRING),
      com.google.cloud.bigquery.Field
        .of("value", com.google.cloud.bigquery.StandardSQLTypeName.STRING)
    )
    val tableDefinition = com.google.cloud.bigquery.StandardTableDefinition.of(schema)
    val tableInfo = com.google.cloud.bigquery.TableInfo.newBuilder(tableId, tableDefinition).build()
    bigquery.create(tableInfo)

    // Insert rows in batches of 50 using INSERT + UNION ALL
    val batchSize = 50
    (1 to count).grouped(batchSize).foreach { batch =>
      val rows = batch
        .map(i => s"SELECT '${tableName}_key_$i' AS key, '${UUID.randomUUID()}_$i' AS value")
        .mkString(" UNION ALL ")
      val sql = s"INSERT INTO `$bqProject.$bqDataset.$tableName` (key, value) $rows"
      val queryConfig = com.google.cloud.bigquery.QueryJobConfiguration.newBuilder(sql)
        .setUseLegacySql(false)
        .build()
      bigquery.query(queryConfig)
    }
  }

  private def countBigQueryRows(suffix: String): Long = {
    val bigquery =
      com.google.cloud.bigquery.BigQueryOptions.newBuilder
        .setProjectId(bqProject)
        .build()
        .getService
    val sql = s"SELECT COUNT(*) AS cnt FROM `${bqTableDot(suffix)}`"
    val queryConfig = com.google.cloud.bigquery.QueryJobConfiguration.newBuilder(sql)
      .setUseLegacySql(false)
      .build()
    val result = bigquery.query(queryConfig)
    result.iterateAll().iterator().next().get("cnt").getLongValue
  }

  private def deleteTopicAndWait(
    adminClient: AdminClient,
    topicName: String,
    timeoutSeconds: Long = 30L
  ): Unit = {
    import java.util.concurrent.TimeUnit
    val found =
      adminClient.listTopics().names().get(timeoutSeconds, TimeUnit.SECONDS).contains(topicName)
    if (found) {
      adminClient.deleteTopics(java.util.Collections.singleton(topicName))
      var retries = 20
      while (retries > 0) {
        Thread.sleep(500)
        try {
          val topics = adminClient.listTopics().names().get(timeoutSeconds, TimeUnit.SECONDS)
          if (!topics.contains(topicName)) retries = 0
          else retries -= 1
        } catch {
          case _: Exception => retries -= 1
        }
      }
    }
  }

  def kafkaConfig(cometOffsetsMode: String, cometOffsetTopicName: String) = ConfigFactory
    .parseString(s"""
         |kafka {
         |  serverOptions = {
         |      "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |  }
         |  cometOffsetsMode = "$cometOffsetsMode"
         |  topics {
         |    "test_bq_offload": {
         |      topicName: "test_bq_offload"
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
         |        "subscribe": "test_bq_offload"
         |        "startingOffsets": "earliest"
         |      }
         |    },
         |    "test_bq_source_output": {
         |      topicName: "test_bq_source_output"
         |      maxRead = 0
         |      fields = ["cast(key as STRING)", "cast(value as STRING)"]
         |      accessOptions = {
         |        "kafka.bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "bootstrap.servers": "${kafkaContainer.bootstrapServers}"
         |        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"
         |        "key.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
         |        "subscribe": "test_bq_source_output"
         |        "startingOffsets": "earliest"
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
         |""".stripMargin)
    .withFallback(super.testConfiguration)

  private def createTempJsonDataFile(count: Int): File = {
    val elems = ListBuffer[String]()
    for (i <- 1 to count) {
      elems += s"""{"key": "key-$i","value": "${UUID.randomUUID().toString}-$i"}"""
    }
    val file = File(s"/tmp/kafka_bq_test_$runId.json")
    file.delete(swallowIOExceptions = true)
    file.overwrite(elems.mkString("\n"))
    file
  }

  // ---------------------------------------------------------------------------
  // Validation tests (Task 10) -- DO NOT require BQ env vars
  // ---------------------------------------------------------------------------
  "BigQuery sink with missing table" should "fail validation" in {
    new WithSettings(kafkaConfig("FILE", s"/tmp/comet_offsets_bq_$runId")) {
      val kafkaJob = new KafkaJob(
        KafkaJobConfig(
          topicConfigName = Some("test_bq_offload"),
          format = "kafka",
          writeMode = SaveMode.Append.toString,
          writeFormat = "bigquery",
          writeOptions = Map(
            "temporaryGcsBucket" -> "some-bucket"
          )
        ),
        schemaHandler = settings.schemaHandler()
      )
      val result = kafkaJob.run()
      result shouldBe a[Failure[_]]
      result.failed.get.getMessage should include("table")
    }
  }

  "BigQuery sink with missing temporaryGcsBucket" should "fail validation" in {
    new WithSettings(kafkaConfig("FILE", s"/tmp/comet_offsets_bq_$runId")) {
      val kafkaJob = new KafkaJob(
        KafkaJobConfig(
          topicConfigName = Some("test_bq_offload"),
          format = "kafka",
          writeMode = SaveMode.Append.toString,
          writeFormat = "bigquery",
          writeOptions = Map(
            "table" -> "project:dataset.table"
          )
        ),
        schemaHandler = settings.schemaHandler()
      )
      val result = kafkaJob.run()
      result shouldBe a[Failure[_]]
      result.failed.get.getMessage should include("temporaryGcsBucket")
    }
  }

  "BigQuery as streaming source" should "fail validation" in {
    new WithSettings(kafkaConfig("FILE", s"/tmp/comet_offsets_bq_$runId")) {
      val kafkaJob = new KafkaJob(
        KafkaJobConfig(
          format = "bigquery",
          path = Some("project.dataset.table"),
          streaming = true,
          writeMode = SaveMode.Append.toString,
          writeFormat = "kafka",
          writeOptions = Map(
            "topic"                   -> "test_bq_source_output",
            "kafka.bootstrap.servers" -> kafkaContainer.bootstrapServers
          )
        ),
        schemaHandler = settings.schemaHandler()
      )
      val result = kafkaJob.run()
      result shouldBe a[Failure[_]]
      result.failed.get.getMessage should include("streaming source")
    }
  }

  "BigQuery source with missing path and query" should "fail validation" in {
    new WithSettings(kafkaConfig("FILE", s"/tmp/comet_offsets_bq_$runId")) {
      val kafkaJob = new KafkaJob(
        KafkaJobConfig(
          format = "bigquery",
          writeMode = SaveMode.Append.toString,
          writeFormat = "kafka",
          writeOptions = Map(
            "topic"                   -> "test_bq_source_output",
            "kafka.bootstrap.servers" -> kafkaContainer.bootstrapServers
          )
        ),
        schemaHandler = settings.schemaHandler()
      )
      val result = kafkaJob.run()
      result shouldBe a[Failure[_]]
      result.failed.get.getMessage should include("--path")
    }
  }

  // ---------------------------------------------------------------------------
  // BQ integration tests -- gated by env vars
  // ---------------------------------------------------------------------------
  if (
    sys.env.get("SL_KAFKA_BQ_TEST_DISABLE").isEmpty &&
    bqProject.nonEmpty &&
    bqBucket.nonEmpty
  ) {

    def bqTests(cometOffsetsMode: String, cometOffsetTopicName: String): WithSettings =
      new WithSettings(kafkaConfig(cometOffsetsMode, cometOffsetTopicName)) {

        private def cleanOffsets(): Unit = {
          if (cometOffsetsMode == "FILE")
            File(cometOffsetTopicName).delete(swallowIOExceptions = true)
        }

        private def withKafkaClient[T](f: KafkaClient => T): T = {
          val kafkaClient = new KafkaClient(settings.appConfig.kafka)
          try {
            f(kafkaClient)
          } finally {
            kafkaClient.close()
          }
        }

        // Test 1 -- Kafka to BQ batch append (Task 6)
        s"$cometOffsetsMode Kafka to BigQuery batch append" should "write 100 rows" in {
          cleanOffsets()
          withKafkaClient { kafkaClient =>
            val topicName = "test_bq_offload"
            deleteTopicAndWait(kafkaClient.client, topicName)
            kafkaClient.createTopicIfNotPresent(
              new NewTopic(topicName, 1, 1.toShort),
              Map.empty
            )

            val file = createTempJsonDataFile(100)
            // Load 100 messages into Kafka topic
            val loadJob = new KafkaJob(
              KafkaJobConfig(
                path = Some(file.pathAsString),
                format = "json",
                writeTopicConfigName = Some(topicName),
                writeMode = SaveMode.Append.toString,
                writeFormat = "kafka"
              ),
              schemaHandler = settings.schemaHandler()
            )
            loadJob.run() match {
              case Success(_) => // ok
              case Failure(e) => throw e
            }

            val suffix = "append"
            createdTables += suffix
            // Offload from Kafka to BQ
            val offloadJob = new KafkaJob(
              KafkaJobConfig(
                topicConfigName = Some(topicName),
                format = "kafka",
                writeMode = SaveMode.Append.toString,
                writeFormat = "bigquery",
                writeOptions = Map(
                  "table"              -> bqTableRef(suffix),
                  "temporaryGcsBucket" -> bqBucket
                )
              ),
              schemaHandler = settings.schemaHandler()
            )
            offloadJob.run() match {
              case Success(_) =>
                countBigQueryRows(suffix) shouldBe 100
              case Failure(e) => throw e
            }
          }
        }

        // Test 2 -- Kafka to BQ batch overwrite (Task 7)
        s"$cometOffsetsMode Kafka to BigQuery batch overwrite" should "keep only last batch" in {
          cleanOffsets()
          withKafkaClient { kafkaClient =>
            val topicName = "test_bq_offload"
            deleteTopicAndWait(kafkaClient.client, topicName)
            kafkaClient.createTopicIfNotPresent(
              new NewTopic(topicName, 1, 1.toShort),
              Map.empty
            )

            val suffix = "overwrite"
            createdTables += suffix

            // First batch: 50 messages
            val file1 = createTempJsonDataFile(50)
            val loadJob1 = new KafkaJob(
              KafkaJobConfig(
                path = Some(file1.pathAsString),
                format = "json",
                writeTopicConfigName = Some(topicName),
                writeMode = SaveMode.Append.toString,
                writeFormat = "kafka"
              ),
              schemaHandler = settings.schemaHandler()
            )
            loadJob1.run() match {
              case Success(_) => // ok
              case Failure(e) => throw e
            }

            val offloadJob1 = new KafkaJob(
              KafkaJobConfig(
                topicConfigName = Some(topicName),
                format = "kafka",
                writeMode = SaveMode.Overwrite.toString,
                writeFormat = "bigquery",
                writeOptions = Map(
                  "table"              -> bqTableRef(suffix),
                  "temporaryGcsBucket" -> bqBucket
                )
              ),
              schemaHandler = settings.schemaHandler()
            )
            offloadJob1.run() match {
              case Success(_) => // ok
              case Failure(e) => throw e
            }

            // Second batch: 30 messages -- overwrite
            deleteTopicAndWait(kafkaClient.client, topicName)
            kafkaClient.createTopicIfNotPresent(
              new NewTopic(topicName, 1, 1.toShort),
              Map.empty
            )
            cleanOffsets()

            val file2 = createTempJsonDataFile(30)
            val loadJob2 = new KafkaJob(
              KafkaJobConfig(
                path = Some(file2.pathAsString),
                format = "json",
                writeTopicConfigName = Some(topicName),
                writeMode = SaveMode.Append.toString,
                writeFormat = "kafka"
              ),
              schemaHandler = settings.schemaHandler()
            )
            loadJob2.run() match {
              case Success(_) => // ok
              case Failure(e) => throw e
            }

            val offloadJob2 = new KafkaJob(
              KafkaJobConfig(
                topicConfigName = Some(topicName),
                format = "kafka",
                writeMode = SaveMode.Overwrite.toString,
                writeFormat = "bigquery",
                writeOptions = Map(
                  "table"              -> bqTableRef(suffix),
                  "temporaryGcsBucket" -> bqBucket
                )
              ),
              schemaHandler = settings.schemaHandler()
            )
            offloadJob2.run() match {
              case Success(_) =>
                countBigQueryRows(suffix) shouldBe 30
              case Failure(e) => throw e
            }
          }
        }

        // Test 3 -- BigQuery table to Kafka (Task 8)
        s"$cometOffsetsMode BigQuery table to Kafka" should "produce 50 messages" in {
          cleanOffsets()
          withKafkaClient { kafkaClient =>
            val suffix = "tbl_source"
            seedBigQueryTable(suffix, 50)

            val outputTopic = "test_bq_source_output"
            deleteTopicAndWait(kafkaClient.client, outputTopic)
            kafkaClient.createTopicIfNotPresent(
              new NewTopic(outputTopic, 1, 1.toShort),
              Map.empty
            )

            val kafkaJob = new KafkaJob(
              KafkaJobConfig(
                format = "bigquery",
                path = Some(bqTableDot(suffix)),
                writeTopicConfigName = Some(outputTopic),
                writeMode = SaveMode.Append.toString,
                writeFormat = "kafka"
              ),
              schemaHandler = settings.schemaHandler()
            )
            kafkaJob.run() match {
              case Success(_) =>
                val offsets = kafkaClient.topicEndOffsets(
                  outputTopic,
                  settings.appConfig.kafka
                    .topics(outputTopic)
                    .allAccessOptions()
                )
                offsets.map(_._2).sum shouldBe 50
              case Failure(e) => throw e
            }
          }
        }

        // Test 4 -- BigQuery SQL query to Kafka (Task 9)
        s"$cometOffsetsMode BigQuery SQL query to Kafka" should "produce 10 messages" in {
          cleanOffsets()
          withKafkaClient { kafkaClient =>
            val suffix = "sql_source"
            seedBigQueryTable(suffix, 100)

            val outputTopic = "test_bq_source_output"
            deleteTopicAndWait(kafkaClient.client, outputTopic)
            kafkaClient.createTopicIfNotPresent(
              new NewTopic(outputTopic, 1, 1.toShort),
              Map.empty
            )

            val query =
              s"SELECT key, value FROM `${bqTableDot(suffix)}` LIMIT 10"

            val kafkaJob = new KafkaJob(
              KafkaJobConfig(
                format = "bigquery",
                options = Map(
                  "query"              -> query,
                  "temporaryGcsBucket" -> bqBucket
                ),
                writeTopicConfigName = Some(outputTopic),
                writeMode = SaveMode.Append.toString,
                writeFormat = "kafka"
              ),
              schemaHandler = settings.schemaHandler()
            )
            kafkaJob.run() match {
              case Success(_) =>
                val offsets = kafkaClient.topicEndOffsets(
                  outputTopic,
                  settings.appConfig.kafka
                    .topics(outputTopic)
                    .allAccessOptions()
                )
                offsets.map(_._2).sum shouldBe 10
              case Failure(e) => throw e
            }
          }
        }

        // Test 6 -- Offset tracking (Task 11)
        s"$cometOffsetsMode Kafka to BigQuery offset tracking" should "not reprocess on second run" in {
          cleanOffsets()
          withKafkaClient { kafkaClient =>
            val topicName = "test_bq_offload"
            deleteTopicAndWait(kafkaClient.client, topicName)
            kafkaClient.createTopicIfNotPresent(
              new NewTopic(topicName, 1, 1.toShort),
              Map.empty
            )

            val suffix = "offsets"
            createdTables += suffix

            val file = createTempJsonDataFile(50)
            // Load 50 messages into Kafka
            val loadJob = new KafkaJob(
              KafkaJobConfig(
                path = Some(file.pathAsString),
                format = "json",
                writeTopicConfigName = Some(topicName),
                writeMode = SaveMode.Append.toString,
                writeFormat = "kafka"
              ),
              schemaHandler = settings.schemaHandler()
            )
            loadJob.run() match {
              case Success(_) => // ok
              case Failure(e) => throw e
            }

            // First offload: 50 rows -> BQ
            val offloadConfig = KafkaJobConfig(
              topicConfigName = Some(topicName),
              format = "kafka",
              writeMode = SaveMode.Append.toString,
              writeFormat = "bigquery",
              writeOptions = Map(
                "table"              -> bqTableRef(suffix),
                "temporaryGcsBucket" -> bqBucket
              )
            )
            val offloadJob1 = new KafkaJob(offloadConfig, schemaHandler = settings.schemaHandler())
            offloadJob1.run() match {
              case Success(_) =>
                countBigQueryRows(suffix) shouldBe 50
              case Failure(e) => throw e
            }

            // Second offload with same config -- no new messages, offsets already saved
            val offloadJob2 = new KafkaJob(offloadConfig, schemaHandler = settings.schemaHandler())
            offloadJob2.run() match {
              case Success(_) =>
                countBigQueryRows(suffix) shouldBe 50
              case Failure(e) => throw e
            }
          }
        }

        // Test 7 -- Kafka to BQ streaming (Task 12)
        s"$cometOffsetsMode Kafka to BigQuery streaming" should "write 75 rows" in {
          cleanOffsets()
          withKafkaClient { kafkaClient =>
            val topicName = "test_bq_offload"
            deleteTopicAndWait(kafkaClient.client, topicName)
            kafkaClient.createTopicIfNotPresent(
              new NewTopic(topicName, 1, 1.toShort),
              Map.empty
            )

            val file = createTempJsonDataFile(75)
            // Load 75 messages into Kafka topic
            val loadJob = new KafkaJob(
              KafkaJobConfig(
                path = Some(file.pathAsString),
                format = "json",
                writeTopicConfigName = Some(topicName),
                writeMode = SaveMode.Append.toString,
                writeFormat = "kafka"
              ),
              schemaHandler = settings.schemaHandler()
            )
            loadJob.run() match {
              case Success(_) => // ok
              case Failure(e) => throw e
            }

            val suffix = "streaming"
            createdTables += suffix
            // Streaming offload from Kafka to BQ with trigger Once
            val streamJob = new KafkaJob(
              KafkaJobConfig(
                topicConfigName = Some(topicName),
                format = "kafka",
                streaming = true,
                streamingTrigger = Some("Once"),
                writeMode = SaveMode.Append.toString,
                writeFormat = "bigquery",
                writeOptions = Map(
                  "table"              -> bqTableRef(suffix),
                  "temporaryGcsBucket" -> bqBucket,
                  "checkpointLocation" -> s"/tmp/kafka_bq_checkpoint_$runId"
                )
              ),
              schemaHandler = settings.schemaHandler()
            )
            streamJob.run() match {
              case Success(_) =>
                countBigQueryRows(suffix) shouldBe 75
              case Failure(e) => throw e
            }
          }
        }
      }

    bqTests("FILE", s"/tmp/comet_offsets_bq_$runId")
  }
}