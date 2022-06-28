package ai.starlake.job.sink.kafka

import ai.starlake.config.Settings
import ai.starlake.utils.CliConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scopt.OParser

case class KafkaJobConfig(
  topicConfigName: String = "",
  format: String = "parquet",
  mode: SaveMode = SaveMode.Append,
  options: Map[String, String] = Map.empty,
  path: String = "",
  transform: Option[String] = None,
  offload: Boolean = true,
  streaming: Boolean = false,
  streamingWriteFormat: String = "console",
  streamingWriteMode: String = "append",
  writeOptions: Map[String, String] = Map.empty,
  streamingTrigger: Option[String] = Some("Once"), // Once, ProcessingTime, Continuous or None
  streamingTriggerOption: String = "10 seconds",
  streamingWritePartitionBy: Seq[String] = Nil,
  streamingWriteToTable: Boolean = false,
  coalesce: Option[Int] = None
)
trait DataFrameTransform {
  def transform(dataFrame: DataFrame, session: SparkSession): DataFrame
  def configure(topicConfig: Settings.KafkaTopicConfig): DataFrameTransform = this
}

abstract class AvroDataFrameTransform extends DataFrameTransform {
  def transform(dataFrame: DataFrame, session: SparkSession): DataFrame = {
    dataFrame
  }
}

object KafkaJobConfig extends CliConfig[KafkaJobConfig] {

  val parser: OParser[Unit, KafkaJobConfig] = {
    val builder = OParser.builder[KafkaJobConfig]
    import builder._
    OParser.sequence(
      programName("starlake kafkaload"),
      head("starlake", "kafkaload", "[options]"),
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
        .action((x, c) => c.copy(topicConfigName = x))
        .text("Topic Name declared in reference.conf file")
        .required(),
      opt[String]("format")
        .action((x, c) => c.copy(format = x))
        .text("Read/Write format eq : parquet, json, csv ... Default to parquet.")
        .optional(),
      opt[String]("path")
        .action((x, c) => c.copy(path = x))
        .text("Source file for load and target file for store")
        .required(),
      opt[String]("mode")
        .action((x, c) => c.copy(mode = SaveMode.valueOf(x)))
        .text(
          "When offload is true, describes how data should be stored on disk. Ignored if offload is false."
        )
        .required(),
      opt[Map[String, String]]("write-options")
        .action((x, c) => c.copy(writeOptions = x))
        .text(
          "Options to pass to Spark Writer"
        )
        .optional(),
      opt[Map[String, String]]("options")
        .action((x, c) => c.copy(options = x))
        .text(
          "Options to pass to Spark Reader"
        )
        .optional(),
      opt[Int]("coalesce")
        .action((x, c) => c.copy(coalesce = Some(x)))
        .text(
          "Should we coalesce the resulting dataframe"
        )
        .optional(),
      opt[String]("transform")
        .action((x, c) => c.copy(transform = Some(x)))
        .text("Any transformation to apply to message before loading / offloading it"),
      opt[Boolean]("offload")
        .action((x, c) => c.copy(offload = x))
        .text(
          "If true, kafka topic is offloaded to path, else data contained in path is stored in the kafka topic"
        )
        .optional()
        .children(
          opt[Unit]("stream")
            .action((_, c) => c.copy(streaming = true))
            .text(
              "Should we use streaming mode ?"
            )
            .optional()
            .children(
              opt[String]("streaming-format")
                .action((x, c) => c.copy(streamingWriteFormat = x))
                .text(
                  "Streaming format eq. kafka, console ..."
                )
                .optional(),
              opt[String]("streaming-output-mode")
                .action((x, c) => c.copy(streamingWriteMode = x))
                .text(
                  "Output mode : eq. append ... "
                )
                .optional(),
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
    )
  }

  // comet kafkaload  --topic xxx
  def parse(args: Seq[String]): Option[KafkaJobConfig] =
    OParser.parse(parser, args, KafkaJobConfig())
}
