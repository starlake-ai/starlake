package ai.starlake.job.convert

import ai.starlake.utils.CliConfig
import org.apache.hadoop.fs.Path
import scopt.OParser

case class FileSplitterConfig(
  inputFile: Path = new Path(""),
  outputFolder: Path = new Path(""),
  split: String = "_split",
  separator: Option[String] = None,
  start: Option[Int] = None,
  end: Option[Int] = None
)

object FileSplitterConfig extends CliConfig[FileSplitterConfig] {
  val command = "splitfile"
  val parser: OParser[Unit, FileSplitterConfig] = {
    val builder = OParser.builder[FileSplitterConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(
        """
          |Split DSV or POSITION files.
          |For DSV files, the value of the first column will be used as the filename
          |For POSITION files, the value between start and end inclusive positions will be used as the filename.
          |All lines with the same value in this column will be saved in the same file.
          |example: starlake filesplit
          |         --input_file /my-path/file.csv
          |         --output_dir /my-path/files/
          |         --option separator=,
        """.stripMargin
      ),
      opt[String]("input_file")
        .action((x, c) => c.copy(inputFile = new Path(x)))
        .text("Full Path to input file to split")
        .required(),
      opt[String]("output_dir")
        .action((x, c) => c.copy(outputFolder = new Path(x)))
        .text(
          "Full Path to output directory, where the output split should be saved with the same extension as the input file if any"
        )
        .required(),
      opt[String]("separator")
        .action((x, c) => c.copy(separator = Some(x)))
        .text("If provided the input file is a file with delimited values")
        .optional(),
      opt[String]("split")
        .action((x, c) => c.copy(split = x))
        .text(
          "If provided the name of the splitted folder in format HIVE (/output-folder/_split=AZ/... "
        )
        .optional(),
      opt[Int]("start")
        .action((x, c) => c.copy(start = Some(x)))
        .text(
          s"If provided, the file is a position file and the end position should be provided too"
        )
        .optional(),
      opt[Int]("end")
        .action((x, c) => c.copy(end = Some(x)))
        .text(
          s"If provided, the file is a position file and the start position should be provided too"
        )
        .optional()
    )
  }

  // comet bqload  --source_file xxx --output_dataset domain --output_table schema --source_format parquet --create_disposition  CREATE_IF_NEEDED --write_disposition WRITE_TRUNCATE
  def parse(args: Seq[String]): Option[FileSplitterConfig] = {
    val options = OParser.parse(parser, args, FileSplitterConfig())
    options match {
      case Some(FileSplitterConfig(_, _, _, None, Some(_), Some(_))) | Some(
            FileSplitterConfig(_, _, _, Some(_), None, None)
          ) =>
        options
      case _ =>
        throw new Exception("Invalid options combination")
    }
  }
}
