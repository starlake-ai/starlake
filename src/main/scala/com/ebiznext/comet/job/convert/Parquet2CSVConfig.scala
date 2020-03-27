package com.ebiznext.comet.job.convert

import com.ebiznext.comet.utils.CliConfig
import org.apache.hadoop.fs.Path
import scopt.OParser

case class Parquet2CSVConfig(
  val inputFolder: Path = new Path("/"),
  val outputFolder: Option[Path] = None,
  val domainName: Option[String] = None,
  val schemaName: Option[String] = None,
  val withHeader: Option[Boolean] = None,
  val separator: Option[String] = None,
  val partitions: Option[Int] = None
)

object Parquet2CSVConfig extends CliConfig[Parquet2CSVConfig] {

  val parser: OParser[Unit, Parquet2CSVConfig] = {
    val builder = OParser.builder[Parquet2CSVConfig]
    import builder._
    OParser.sequence(
      programName("comet"),
      head("comet", "1.x"),
      opt[String]("input")
        .action((x, c) => c.copy(inputFolder = new Path(x)))
        .text("Full Path to input folder")
        .required(),
      opt[String]("output_dataset")
        .action((x, c) => c.copy(outputFolder = Some(new Path(x))))
        .text("Full Path to output folder")
        .optional(),
      opt[String]("domain")
        .action((x, c) => c.copy(domainName = Some(x)))
        .text("Domain Name")
        .optional(),
      opt[String]("schema")
        .action((x, c) => c.copy(schemaName = Some(x)))
        .text("Schema Name  ")
        .optional(),
      opt[String]("with_header")
        .action((x, c) => c.copy(withHeader = Some(x.toBoolean)))
        .text("Include header in output file ?")
        .optional(),
      opt[String]("separator")
        .action((x, c) => c.copy(separator = Some(x)))
        .text("Separator to use ?")
        .optional(),
      opt[String]("partitions")
        .action((x, c) => c.copy(partitions = Some(x.toInt)))
        .text("How many output partitions ?")
        .optional()
    )
  }

  // comet bqload  --source_file xxx --output_dataset domain --output_table schema --source_format parquet --create_disposition  CREATE_IF_NEEDED --write_disposition WRITE_TRUNCATE
  def parse(args: Seq[String]): Option[Parquet2CSVConfig] =
    OParser.parse(parser, args, Parquet2CSVConfig())
}
