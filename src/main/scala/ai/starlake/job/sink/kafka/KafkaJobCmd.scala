package ai.starlake.job.sink.kafka

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

object KafkaJobCmd extends Cmd[KafkaJobConfig] {

  val command = "kafkaload"

  val parser: OParser[Unit, KafkaJobConfig] = {
    val builder = OParser.builder[KafkaJobConfig]
    import builder._
    OParser.sequence(
      programName(s"$shell $command"),
      head(shell, command, "[options]"),
      note("""
             |Two modes are available : The batch mode and the streaming mode.
             |
             |### Batch mode
             |In batch mode, you start the kafka (off)loader regurarly and the last consumed offset
             |will be stored in the `comet_offsets` topic config
             |(see [reference-kafka.conf](https://github.com/starlake-ai/starlake/blob/master/src/main/resources/reference-kafka.conf#L22) for an example).
             |
             |When offloading data from kafka to a file, you may ask to coalesce the result to a specific number of files / partitions.
             |If you ask to coalesce to a single partition, the offloader will store the data in the exact filename you provided in the path
             |argument.
             |
             |The figure below describes the batch offloading process
             |![](/img/cli/kafka-offload.png)
             |
             |The figure below describes the batch offloading process with `comet-offsets-mode = "FILE"`
             |![](/img/cli/kafka-offload-fs.png)
             |
             |### Streaming mode
             |
             |In this mode, te program keep running and you the comet_offsets topic is not used. The (off)loader will use a consumer group id
             |you specify in the access options of the topic configuration you are dealing with.
             |""".stripMargin),
      opt[String]("config")
        .action((x, c) => c.copy(topicConfigName = Some(x)))
        .text("Topic Name declared in reference.conf file")
        .optional(),
      opt[String]("connectionRef")
        .action((x, c) => c.copy(connectionRef = Some(x)))
        .text("Connection to any specific sink")
        .optional(),
      opt[String]("format")
        .action((x, c) => c.copy(format = x))
        .text("Read/Write format eq : parquet, json, csv ... Default to parquet.")
        .optional(),
      opt[String]("path")
        .action((x, c) => c.copy(path = Some(x)))
        .text("Source file for load and target file for store")
        .optional(),
      opt[Map[String, String]]("options")
        .action((x, c) => c.copy(options = x))
        .text(
          "Options to pass to Spark Reader"
        )
        .optional(),
      opt[String]("write-config")
        .action((x, c) => c.copy(writeTopicConfigName = Some(x)))
        .text("Topic Name declared in reference.conf file")
        .optional(),
      opt[String]("write-path")
        .action((x, c) => c.copy(writePath = Some(x)))
        .text("Source file for load and target file for store")
        .optional(),
      opt[String]("write-mode")
        .action((x, c) => c.copy(writeMode = x))
        .text(
          "When offload is true, describes how data should be stored on disk. Ignored if offload is false."
        )
        .optional(),
      opt[Map[String, String]]("write-options")
        .action((x, c) => c.copy(writeOptions = x))
        .text(
          "Options to pass to Spark Writer"
        )
        .optional(),
      opt[String]("write-format")
        .action((x, c) => c.copy(writeFormat = x))
        .text(
          "Streaming format eq. kafka, console ..."
        )
        .optional(),
      opt[Int]("write-coalesce")
        .action((x, c) => c.copy(coalesce = Some(x)))
        .text(
          "Should we coalesce the resulting dataframe"
        )
        .optional(),
      opt[String]("transform")
        .action((x, c) => c.copy(transform = Some(x)))
        .text("Any transformation to apply to message before loading / offloading it")
        .optional(),
      opt[Unit]("stream")
        .action((_, c) => c.copy(streaming = true))
        .text(
          "Should we use streaming mode ?"
        )
        .optional()
        .children(
          opt[String]("streaming-trigger")
            .action((x, c) => c.copy(streamingTrigger = Some(x)))
            .text("Once / Continuous / ProcessingTime")
            .optional(),
          opt[String]("streaming-trigger-option")
            .action((x, c) => c.copy(streamingTriggerOption = x))
            .text(
              "10 seconds for example. see https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/streaming/Trigger.html#ProcessingTime-java.lang.String-"
            )
            .optional(),
          opt[Boolean]("streaming-to-table")
            .action { (x, c) =>
              if (x) {
                throw new Exception(
                  "Streaming to a table is still unsupported, reserved for future use"
                )
              }
              c.copy(streamingWriteToTable = x)
            }
            .text("Table name to sink to")
            .optional(),
          opt[Seq[String]]("streaming-partition-by")
            .action((x, c) => c.copy(streamingWritePartitionBy = x))
            .text("List of columns to use for partitioning")
            .optional()
        )
    )
  }

  // comet kafkaload  --topic xxx
  def parse(args: Seq[String]): Option[KafkaJobConfig] =
    OParser.parse(parser, args, KafkaJobConfig(), setup)

  override def run(config: KafkaJobConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).kafkaload(config).map(_ => JobResult.empty)
}
