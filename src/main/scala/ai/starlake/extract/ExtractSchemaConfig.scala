package ai.starlake.extract

import ai.starlake.utils.CliConfig
import scopt.OParser

case class ExtractSchemaConfig(
  extractConfig: String = "",
  outputDir: Option[String] = None,
  parallelism: Option[Int] = None
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
      opt[String]("config")
        .action((x, c) => c.copy(extractConfig = x))
        .required()
        .text("Database tables & connection info"),
      opt[String]("output-dir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .optional()
        .text("Where to output YML files"),
      opt[Int]("parallelism")
        .action((x, c) => c.copy(parallelism = Some(x)))
        .optional()
        .text(
          s"parallelism level of the extraction process. By default equals to the available cores: ${Runtime.getRuntime().availableProcessors()}"
        )
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
