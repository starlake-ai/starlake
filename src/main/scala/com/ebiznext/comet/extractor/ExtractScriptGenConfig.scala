package com.ebiznext.comet.extractor

import better.files.File
import scopt.{OParser, RenderingMode}

case class ExtractScriptGenConfig(
  domain: String = "",
  scriptTemplateFile: File = File("."),
  scriptOutputDir: File = File("."),
  deltaColumn: Option[String] = None
)

object ExtractScriptGenConfig {

  val builder = OParser.builder[ExtractScriptGenConfig]

  def exists(name: String)(path: String): Either[String, Unit] =
    if (File(path).exists) Right(())
    else Left(s"$name at path $path does not exist")

  val parser: OParser[Unit, ExtractScriptGenConfig] = {
    import builder._
    OParser.sequence(
      programName("comet"),
      head("comet", "1.x"),
      note(
        """
          |The schemas should at least, specify :
          |     - a table name (schemas.name)
          |     - a file pattern (schemas.pattern) which is used as the export file base name
          |     - a write mode (schemas.metadata.write): APPEND or OVERWRITE
          |     - a delta column (schemas.merge.timestamp) if in APPEND mode : the default column which is used to determine new rows for each exports
          |     - the columns to extract (schemas.attributes.name*)
          |
          |You also have to provide a Mustache (http://mustache.github.io/mustache.5.html) template file.
          |
          |In there you'll write your extraction export process (sqlplus for Oracle, pgsql for PostgreSQL as an example).
          |In that template you can use the following parameters:
          | - table_name  -> the table to export
          | - delimiter   -> the resulting dsv file delimiter
          | - columns     -> the columns to export
          |   columns is a Mustache map, it gives you access, for each column, to:
          |    - name               -> the column name
          |    - trailing_col_char  -> the separator to append to the column (, if there are more columns to come, "" otherwise)
          |                            Here is an example how to use it in a template:
          |                              SELECT
          |                              {{#columns}}
          |                              TO_CHAR({{name}}){{trailing_col_char}}
          |                              {{/columns}}
          |                              FROM
          |                              {{table_name}};
          | export_file -> the export file name
          | full_export -> if the export is a full or delta export (the logic is to be implemented in your script)
          |""".stripMargin
      ),
      cmd("script-gen"),
      opt[String]("domain")
        .action((x, c) => c.copy(domain = x))
        .required()
        .text("The domain for which to generate extract scripts"),
      opt[String]("templateFile")
        .validate(exists("Script template file"))
        .action((x, c) => c.copy(scriptTemplateFile = File(x)))
        .required()
        .text("Script template file"),
      opt[String]("scriptsOutputDir")
        .validate(exists("Script output folder"))
        .action((x, c) => c.copy(scriptOutputDir = File(x)))
        .required()
        .text("Scripts output folder"),
      opt[String]("deltaColumn")
        .action((x, c) => c.copy(deltaColumn = Some(x)))
        .optional()
        .text("""The default date column used to determine new rows to export.
            |Overrides config database-extractor.default-column value.""".stripMargin)
    )
  }
  val usage: String = OParser.usage(parser, RenderingMode.TwoColumns)

  /** Function to parse command line arguments (domain and schema).
    *
    * @param args : Command line parameters
    * @return : an Option of MetricConfing with the parsed domain and schema names.
    */
  def parse(args: Seq[String]): Option[ExtractScriptGenConfig] =
    OParser.parse(parser, args, ExtractScriptGenConfig.apply())
}
