package com.ebiznext.comet.job.index.kafkaload

import com.ebiznext.comet.utils.CliConfig
import org.apache.spark.sql.{DataFrame, SaveMode}
import scopt.OParser

case class KafkaJobConfig(
  topic: String = "",
  format: String = "parquet",
  mode: SaveMode = SaveMode.Append,
  input: Option[Either[String, DataFrame]] = None,
  offload: Boolean = true
)

object KafkaJobConfig extends CliConfig[KafkaJobConfig] {

  val parser: OParser[Unit, KafkaJobConfig] = {
    val builder = OParser.builder[KafkaJobConfig]
    import builder._
    OParser.sequence(
      programName("comet kafkaload"),
      head("comet", "kafkaload", "[options]"),
      note(""),
      opt[String]("topic")
        .action((x, c) => c.copy(topic = x))
        .text("Topic Name declared in reference.conf file")
        .required(),
      opt[String]("format")
        .action((x, c) => c.copy(topic = x))
        .text("Read/Write format eq : parquet, json, csv ... Default to parquet.")
        .optional(),
      opt[String]("path")
        .action((x, c) => c.copy(input = Some(Left(x))))
        .text("Source file for load and target file for store")
        .required(),
      opt[String]("mode")
        .action((x, c) => c.copy(mode = SaveMode.valueOf(x)))
        .text("Source file for load and target file for store")
        .required(),
      opt[Boolean]("offload")
        .action((x, c) => c.copy(offload = x))
        .text(
          "If true, kafka topic is offloaded to path, else data contained in path is stored in the kafka topic"
        )
        .optional()
    )
  }

  // comet kafkaload  --topic xxx
  def parse(args: Seq[String]): Option[KafkaJobConfig] =
    OParser.parse(parser, args, KafkaJobConfig())
}
