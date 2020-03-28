package com.ebiznext.comet.job.convert

import com.ebiznext.comet.schema.model.WriteMode
import com.ebiznext.comet.utils.CliConfig
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import scopt.OParser

case class Parquet2CSVConfig(
  val inputFolder: Path = new Path("/"),
  val outputFolder: Option[Path] = None,
  val domainName: Option[String] = None,
  val schemaName: Option[String] = None,
  val withHeader: Boolean = false,
  val writeMode: Option[WriteMode] = None,
  val separator: String = ",",
  val partitions: Int = 1
)

object Parquet2CSVConfig extends CliConfig[Parquet2CSVConfig] {

  val parser: OParser[Unit, Parquet2CSVConfig] = {
    val builder = OParser.builder[Parquet2CSVConfig]
    import builder._
    OParser.sequence(
      programName("comet"),
      head("comet", "1.x"),
      opt[String]("input_folder")
        .action((x, c) => c.copy(inputFolder = new Path(x)))
        .text("Full Path to input folder")
        .required(),
      opt[String]("output_folder")
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
      opt[Unit]("with_header")
        .action((_, c) => c.copy(withHeader = true))
        .text("Include header in output file ?")
        .optional(),
      opt[String]("write_mode")
        .action((x, c) => c.copy(writeMode = Some(WriteMode.fromString(x))))
        .text(s"One of ${WriteMode.writes}")
        .optional(),
      opt[String]("separator")
        .action((x, c) => c.copy(separator = x))
        .text("Separator to use")
        .optional(),
      opt[String]("partitions")
        .action((x, c) => c.copy(partitions = x.toInt))
        .text("How many output partitions")
        .optional()
    )
  }

  // comet bqload  --source_file xxx --output_dataset domain --output_table schema --source_format parquet --create_disposition  CREATE_IF_NEEDED --write_disposition WRITE_TRUNCATE
  def parse(args: Seq[String]): Option[Parquet2CSVConfig] =
    OParser.parse(parser, args, Parquet2CSVConfig())
}
