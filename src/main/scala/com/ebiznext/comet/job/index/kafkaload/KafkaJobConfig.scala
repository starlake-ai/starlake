package com.ebiznext.comet.job.index.kafkaload

import com.ebiznext.comet.utils.{CliConfig, Utils}
import org.apache.spark.sql.{DataFrame, SaveMode}
import scopt.OParser

case class KafkaJobConfig(
  topic: String = "",
  format: String = "parquet",
  mode: SaveMode = SaveMode.Append,
  path: Option[String] = None,
  transform: Option[String] = None,
  offload: Boolean = true
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
        .action((x, c) => c.copy(topic = x))
        .text("Topic Name declared in reference.conf file")
        .required(),
      opt[String]("format")
        .action((x, c) => c.copy(topic = x))
        .text("Read/Write format eq : parquet, json, csv ... Default to parquet.")
        .optional(),
      opt[String]("path")
        .action((x, c) => c.copy(path = Some(x)))
        .text("Source file for load and target file for store")
        .required(),
      opt[String]("mode")
        .action((x, c) => c.copy(mode = SaveMode.valueOf(x)))
        .text("Source file for load and target file for store")
        .required(),
      opt[String]("transform")
        .action((x, c) => c.copy(transform = Some(x)))
        .text("Any transformation to apply to message before load / offloading it"),
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
