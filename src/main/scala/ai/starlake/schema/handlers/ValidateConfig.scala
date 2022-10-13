package ai.starlake.schema.handlers

import ai.starlake.utils.CliConfig
import scopt.OParser

case class ValidateConfig(reload: Boolean = false)

object ValidateConfig extends CliConfig[ValidateConfig] {
  val command = "serve"
  val parser: OParser[Unit, ValidateConfig] = {
    val builder = OParser.builder[ValidateConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[Unit]("reload")
        .action((x, c) => c.copy(reload = true))
        .optional()
        .text("Port on which the server is listening")
    )
  }

  override def parse(args: Seq[String]): Option[ValidateConfig] =
    OParser.parse(parser, args, ValidateConfig())

}
