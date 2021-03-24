package com.ebiznext.comet.job.index.kafkaload

import com.ebiznext.comet.utils.{CliConfig, Utils}
import org.apache.spark.sql.{DataFrame, SaveMode}
import scopt.OParser

case class KafkaJobConfig(
  topicConfigName: String = "",
  format: String = "parquet",
  mode: SaveMode = SaveMode.Append,
  path: String = "",
  transform: Option[String] = None,
  offload: Boolean = true,
  streaming: Boolean = false,
  streamingWriteFormat: String = "console",
  streamingWriteMode: String = "append",
  writeOptions: Map[String, String] = Map.empty,
  streamingTrigger: String = "Once",
  streamingTriggerOption: String = "",
  streamingWritePartitionBy: Seq[String] = Nil,
  streamingWriteToTable: Boolean = false
) {

  val transformInstance: Option[DataFrameTransform] = {
    transform.map(Utils.loadInstance[DataFrameTransform])
  }
}

trait DataFrameTransform {
  def transform(dataFrame: DataFrame): DataFrame
}

object KafkaJobConfig extends CliConfig[KafkaJobConfig] {

  val parser: OParser[Unit, KafkaJobConfig] = {
    val builder = OParser.builder[KafkaJobConfig]
    import builder._
    OParser.sequence(
      programName("comet kafkaload"),
      head("comet", "kafkaload", "[options]"),
      note(""),
      opt[String]("topic")
        .action((x, c) => c.copy(topicConfigName = x))
        .text("Topic Name declared in reference.conf file")
        .required(),
      opt[String]("format")
        .action((x, c) => c.copy(topicConfigName = x))
        .text("Read/Write format eq : parquet, json, csv ... Default to parquet.")
        .optional(),
      opt[String]("path")
        .action((x, c) => c.copy(path = x))
        .text("Source file for load and target file for store")
        .required(),
      opt[String]("mode")
        .action((x, c) => c.copy(mode = SaveMode.valueOf(x)))
        .text(
          "When offload is true, describes who data should be stored on disk. Ignored if offload is false."
        )
        .required(),
      opt[Map[String, String]]("write-options")
        .action((x, c) => c.copy(writeOptions = x))
        .text(
          "Options to pass to Spark Writer"
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
              "If true, kafka topic is offloaded to path, else data contained in path is stored in the kafka topic"
            )
            .optional()
            .children(
              opt[String]("streaming-format")
                .action((x, c) => c.copy(streamingWriteFormat = x))
                .text(
                  "If true, kafka topic is offloaded to path, else data contained in path is stored in the kafka topic"
                )
                .required(),
              opt[String]("streaming-output-mode")
                .action((x, c) => c.copy(streamingWriteMode = x))
                .text(
                  "If true, kafka topic is offloaded to path, else data contained in path is stored in the kafka topic"
                )
                .required(),
              opt[String]("streaming-trigger")
                .action((x, c) => c.copy(streamingTrigger = x))
                .text("Once / Continuous / ProcessingTime")
                .required(),
              opt[String]("streaming-trigger-option")
                .action((x, c) => c.copy(streamingTriggerOption = x))
                .text(
                  "10 seconds for example. see https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/streaming/Trigger.html#ProcessingTime-java.lang.String-"
                )
                .required(),
              opt[Boolean]("streaming-to-table")
                .action((x, c) => c.copy(streamingWriteToTable = x))
                .text("10 seconds / ")
                .required(),
              opt[Seq[String]]("streaming-partition-by")
                .action((x, c) => c.copy(streamingWritePartitionBy = x))
                .text("10 seconds / ")
                .required()
            )
        )
    )
  }

  // comet kafkaload  --topic xxx
  def parse(args: Seq[String]): Option[KafkaJobConfig] =
    OParser.parse(parser, args, KafkaJobConfig())
}
