package ai.starlake.extract

import ai.starlake.utils.CliConfig
import better.files.File
import scopt.OParser

case class ExtractScriptConfig(
  domain: Seq[String] = Nil,
  scriptTemplateName: String = ".",
  deltaColumn: Option[String] = None,
  auditDB: String = "",
  scriptOutputPattern: Option[String] = None
)

object ExtractScriptConfig extends CliConfig[ExtractScriptConfig] {
  val command = "extract-script"
  def exists(name: String)(path: String): Either[String, Unit] = {
    File(path).createFileIfNotExists(createParents = true)
    if (File(path).exists) Right(())
    else Left(s"$name at path $path could not be created")
  }

  val parser: OParser[Unit, ExtractScriptConfig] = {
    val builder = OParser.builder[ExtractScriptConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(
        """
          |For domain extraction, the schemas should at least, specify :
          |- a table name (schemas.name)
          |- a file pattern (schemas.pattern) which is used as the export file base name
          |- a write mode (schemas.metadata.write): APPEND or OVERWRITE
          |- a delta column (schemas.merge.timestamp) if in APPEND mode : the default column which is used to determine new rows for each exports
          |- the columns to extract (schemas.attributes.name*)
          |
          |You also have to provide a Mustache (http://mustache.github.io/mustache.5.html) template file.
          |
          |In there you'll write your extraction export process (sqlplus for Oracle, pgsql for PostgreSQL as an example).
          |In that template you can use the following parameters:
          |- table_name  -> the table to export
          |- delimiter   -> the resulting dsv file delimiter
          |- columns     -> the columns to export
          |   columns is a Mustache map, it gives you access, for each column, to:
          |    - name               -> the column name
          |    - trailing_col_char  -> the separator to append to the column (, if there are more columns to come, "" otherwise)
          |                            Here is an example how to use it in a template:
          |````sql
          |                              SELECT
          |                              {{#columns}}
          |                              TO_CHAR({{name}}){{trailing_col_char}}
          |                              {{/columns}}
          |                              FROM
          |                              {{table_name}};
          |````
          | full_export -> if the export is a full or delta export (the logic is to be implemented in your script)
          |""".stripMargin
      ),
      cmd("extract-script"),
      opt[Seq[String]]("domain")
        .action((x, c) => c.copy(domain = x))
        .valueName("domain1,domain2 ...")
        .optional()
        .text("The domain list for which to generate extract scripts"),
      opt[String]("template")
        .action((x, c) => c.copy(scriptTemplateName = x))
        .required()
        .text("Script template dir"),
      opt[String]("audit-schema")
        .action((x, c) => c.copy(auditDB = x))
        .required()
        .text("Audit DB that will contain the audit export table"),
      opt[String]("delta-column")
        .action((x, c) => c.copy(deltaColumn = Some(x)))
        .optional()
        .text(
          """The default date column used to determine new rows to export. Overrides config database-extractor.default-column value.""".stripMargin
        )
    )
  }

  /** Function to parse command line arguments (domain and schema).
    *
    * @param args
    *   : Command line parameters
    * @return
    *   : an Option of MetricConfing with the parsed domain and schema names.
    */
  def parse(args: Seq[String]): Option[ExtractScriptConfig] =
    OParser.parse(parser, args, ExtractScriptConfig(), setup)
}
