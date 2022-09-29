package ai.starlake.job.convert

import ai.starlake.schema.model.WriteMode
import ai.starlake.schema.model.WriteMode
import ai.starlake.utils.CliConfig
import org.apache.hadoop.fs.Path
import scopt.OParser

case class Parquet2CSVConfig(
  inputFolder: Path = new Path("/"),
  outputFolder: Option[Path] = None,
  domainName: Option[String] = None,
  schemaName: Option[String] = None,
  writeMode: Option[WriteMode] = None,
  deleteSource: Boolean = false,
  options: List[(String, String)] = Nil,
  partitions: Int = 1
)

object Parquet2CSVConfig extends CliConfig[Parquet2CSVConfig] {
  val command = "parquet2csv"
  val parser: OParser[Unit, Parquet2CSVConfig] = {
    val builder = OParser.builder[Parquet2CSVConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(
        """
          |Convert parquet files to CSV.
          |The folder hierarchy should be in the form /input_folder/domain/schema/part*.parquet
          |Once converted the csv files are put in the folder /output_folder/domain/schema.csv file
          |When the specified number of output partitions is 1 then /output_folder/domain/schema.csv is the file containing the data
          |otherwise, it is a folder containing the part*.csv files.
          |When output_folder is not specified, then the input_folder is used a the base output folder.
          |
          |example: starlake parquet2csv
          |         --input_dir /tmp/datasets/accepted/
          |         --output_dir /tmp/datasets/csv/
          |         --domain sales
          |         --schema orders
          |         --option header=true
          |         --option separator=,
          |         --partitions 1
          |         --write_mode overwrite""".stripMargin
      ),
      opt[String]("input_dir")
        .action((x, c) => c.copy(inputFolder = new Path(x)))
        .text("Full Path to input directory")
        .required(),
      opt[String]("output_dir")
        .action((x, c) => c.copy(outputFolder = Some(new Path(x))))
        .text("Full Path to output directory, if not specified, input_dir is used as output dir")
        .optional(),
      opt[String]("domain")
        .action((x, c) => c.copy(domainName = Some(x)))
        .text(
          "Domain name to convert. All schemas in this domain are converted. If not specified, all schemas of all domains are converted"
        )
        .optional(),
      opt[String]("schema")
        .action((x, c) => c.copy(schemaName = Some(x)))
        .text("Schema name to convert. If not specified, all schemas are converted.")
        .optional(),
      opt[Unit]("delete_source")
        .action((_, c) => c.copy(deleteSource = true))
        .text("Should we delete source parquet files after conversion ?")
        .optional(),
      opt[String]("write_mode")
        .action((x, c) => c.copy(writeMode = Some(WriteMode.fromString(x))))
        .text(s"One of ${WriteMode.writes}")
        .optional(),
      opt[String]("option")
        .unbounded()
        .valueName("spark-option=value")
        .action((x, c) => {
          val option = x.split('=')
          c.copy(options = c.options :+ (option(0) -> option(1)))
        })
        .text("Any Spark option to use (sep, delimiter, quote, quoteAll, escape, header ...)")
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
