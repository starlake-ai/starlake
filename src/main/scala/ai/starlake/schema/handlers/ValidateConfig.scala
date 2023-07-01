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
        .text(
          "Reload all files from disk before starting validation. Always true regardless of the value set here."
        )
    )
  }

  override def parse(args: Seq[String]): Option[ValidateConfig] =
    OParser.parse(parser, args, ValidateConfig())

}
