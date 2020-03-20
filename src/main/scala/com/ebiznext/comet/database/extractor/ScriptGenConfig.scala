package com.ebiznext.comet.database.extractor

import better.files.File
import scopt.{OParser, RenderingMode}

case class ScriptGenConfig(
  referentialFile: File = File("."),
  scriptTemplateFile: File = File("."),
  scriptOutputDir: File = File(".")
)

object ScriptGenConfig {

  val builder = OParser.builder[ScriptGenConfig]

  def exists(name: String)(path: String): Either[String, Unit] =
    if (File(path).exists) Right(())
    else Left(s"$name at path $path does not exist")

  val parser: OParser[Unit, ScriptGenConfig] = {
    import builder._
    OParser.sequence(
      programName("comet"),
      head("comet", "1.x"),
      note(
        """
          |The Excel referential should, at least, specify :
          |   - "schema" sheet
          |     - a table name (col A)
          |     - a file pattern (col B) which is used as the export file base name
          |     - a write mode (col D): APPEND or OVERWRITE
          |     - a delta column (col H) if in APPEND mode : the column which is used to determine new rows for each exports
          |   - in corresponding source (table) sheets:
          |     - the columns to extract
          |
          |You also have to provide a Mustache (http://mustache.github.io/mustache.5.html) template file.
          |
          |In there you'll write your extraction export process (sqlplus for Oracle, pgsql for PostgreSQL as an example).
          |In that template you can use the following parameters:
          | table_name  -> the table to export
          | delimiter   -> the resulting dsv file delimiter
          | columns     -> the columns to export
          | export_file -> the export file name
          | full_export -> if the export is a full or delta export (the logic is to be implemented in your script)
          |""".stripMargin
      ),
      cmd("script-gen"),
      opt[String]("referentialFile")
        .validate(exists("Excel referential file"))
        .action((x, c) => c.copy(referentialFile = File(x)))
        .required()
        .text("Excel referential file"),
      opt[String]("templateFile")
        .validate(exists("Script template file"))
        .action((x, c) => c.copy(scriptTemplateFile = File(x)))
        .required()
        .text("Script template file"),
      opt[String]("scriptsOutputDir")
        .validate(exists("Script output folder"))
        .action((x, c) => c.copy(scriptOutputDir = File(x)))
        .required()
        .text("Scripts output folder")
    )
  }
  val usage: String = OParser.usage(parser, RenderingMode.TwoColumns)

  /** Function to parse command line arguments (domain and schema).
    *
    * @param args : Command line parameters
    * @return : an Option of MetricConfing with the parsed domain and schema names.
    */
  def parse(args: Seq[String]): Option[ScriptGenConfig] =
    OParser.parse(parser, args, ScriptGenConfig.apply())
}
