package com.ebiznext.comet.job.bqload

import buildinfo.BuildInfo
import com.ebiznext.comet.utils.CliConfig
import org.apache.spark.sql.DataFrame
import scopt.OParser

case class BigQueryLoadConfig(
  sourceFile: Either[String, DataFrame] = Left(""),
  outputDataset: String = "",
  outputTable: String = "",
  outputPartition: Option[String] = None,
  sourceFormat: String = "",
  createDisposition: String = "",
  writeDisposition: String = "",
  location: Option[String] = None,
  days: Option[Int] = None
) {
  def getLocation(): String = this.location.getOrElse("EU")
}

object BigQueryLoadConfig extends CliConfig[BigQueryLoadConfig] {

  val parser: OParser[Unit, BigQueryLoadConfig] = {
    val builder = OParser.builder[BigQueryLoadConfig]
    import builder._
    OParser.sequence(
      programName("comet"),
      head("comet", BuildInfo.version),
      opt[String]("source_file")
        .action((x, c) => c.copy(sourceFile = Left(x)))
        .text("Full Path to source file")
        .required(),
      opt[String]("output_dataset")
        .action((x, c) => c.copy(outputDataset = x))
        .text("BigQuery Output Dataset")
        .required(),
      opt[String]("output_table")
        .action((x, c) => c.copy(outputTable = x))
        .text("BigQuery Output Table")
        .required(),
      opt[String]("output_partition")
        .action((x, c) => c.copy(outputPartition = Some(x)))
        .text("BigQuery Partition Field ")
        .optional(),
      opt[String]("source_format")
        .action((x, c) => c.copy(sourceFormat = x))
        .text("Source Format eq. parquet"),
      opt[String]("create_disposition")
        .action((x, c) => c.copy(createDisposition = x))
        .text(
          "Big Query Create disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/CreateDisposition"
        ),
      opt[String]("write_disposition")
        .action((x, c) => c.copy(writeDisposition = x))
        .text(
          "Big Query Write disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition"
        )
    )
  }

  // comet bqload  --source_file xxx --output_dataset domain --output_table schema --source_format parquet --create_disposition  CREATE_IF_NEEDED --write_disposition WRITE_TRUNCATE
  def parse(args: Seq[String]): Option[BigQueryLoadConfig] =
    OParser.parse(parser, args, BigQueryLoadConfig())
}
