package ai.starlake.extract

import ai.starlake.utils.CliConfig
import scopt.OParser

case class ExtractSchemaConfig(
  extractConfig: String = "",
  outputDir: Option[String] = None
)

object ExtractSchemaConfig extends CliConfig[ExtractSchemaConfig] {
  val command = "extract-schema"
  val parser: OParser[Unit, ExtractSchemaConfig] = {
    val builder = OParser.builder[ExtractSchemaConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[String]("mapping")
        .action((x, c) => c.copy(extractConfig = x))
        .optional()
        .text("Deprecated. Use config instead"),
      opt[String]("config")
        .action((x, c) => c.copy(extractConfig = x))
        .required()
        .text("Database tables & connection info"),
      opt[String]("output-dir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .optional()
        .text("Where to output YML files")
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class JDBC2YmlConfig.
    */
  def parse(args: Seq[String]): Option[ExtractSchemaConfig] =
    OParser.parse(parser, args, ExtractSchemaConfig())
}
