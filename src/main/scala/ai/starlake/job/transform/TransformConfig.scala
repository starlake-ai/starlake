package ai.starlake.job.transform

import ai.starlake.utils.CliConfig
import scopt.OParser

case class TransformConfig(
  name: String = "",
  options: Map[String, String] = Map.empty,
  compile: Boolean = false,
  interactive: Option[String] = None,
  reload: Boolean = false,
  truncate: Boolean = false,
  recursive: Boolean = false
)

object TransformConfig extends CliConfig[TransformConfig] {
  val command = "transform"

  val parser: OParser[Unit, TransformConfig] = {
    val builder = OParser.builder[TransformConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[String]("name")
        .action((x, c) => c.copy(name = x))
        .required()
        .text("Task Name"),
      opt[Unit]("compile")
        .action((x, c) => c.copy(compile = true))
        .optional()
        .text("Return final query only"),
      opt[String]("interactive")
        .action((x, c) => c.copy(interactive = Some(x)))
        .optional()
        .text("Run query without sinking the result"),
      opt[Unit]("reload")
        .action((x, c) => c.copy(reload = true))
        .optional()
        .text("Reload YAML  files. Used in server mode"),
      opt[Boolean]("truncate")
        .action((x, c) => c.copy(truncate = x))
        .optional()
        .text(
          s"Force table to be truncated before insert. Default value is false"
        ),
      opt[Unit]("recursive")
        .action((x, c) => c.copy(recursive = true))
        .optional()
        .text(
          s"Execute all dependencies recursively. Default value is false"
        ),
      opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(options = x))
        .text("Job arguments to be used as substitutions")
    )
  }

  def parse(args: Seq[String]): Option[TransformConfig] =
    OParser.parse(parser, args, TransformConfig())
}
