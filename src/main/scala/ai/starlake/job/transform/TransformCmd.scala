package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

trait TransformCmd extends Cmd[TransformConfig] {

  val command = "transform"

  val parser: OParser[Unit, TransformConfig] = {
    val builder = OParser.builder[TransformConfig]
    import builder._
    OParser.sequence(
      programName(s"$shell $command"),
      head(shell, command, "[options]"),
      note(""),
      opt[String]("name")
        .action((x, c) => c.copy(name = x))
        .required()
        .text("Task Name"),
      opt[Unit]("compile")
        .action((_, c) => c.copy(compile = true))
        .optional()
        .text("Return final query only"),
      opt[String]("interactive")
        .action((x, c) => c.copy(interactive = Some(x)))
        .optional()
        .text("Run query without sinking the result"),
      opt[Unit]("reload")
        .action((_, c) => c.copy(reload = true))
        .optional()
        .text("Reload YAML  files. Used in server mode"),
      opt[Boolean]("truncate")
        .action((x, c) => c.copy(truncate = x))
        .optional()
        .text(
          s"Force table to be truncated before insert. Default value is false"
        ),
      opt[Unit]("recursive")
        .action((_, c) => c.copy(recursive = true))
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

  def parse(args: Seq[String]): Option[TransformConfig] = {
    OParser.parse(parser, args, TransformConfig(), setup)
  }

  override def run(config: TransformConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    if (config.compile) {
      workflow(schemaHandler).compileAutoJob(config).map(_ => JobResult.empty)
    } else
      workflow(schemaHandler).autoJob(config).map(_ => JobResult.empty)
  }
}

object TransformCmd extends TransformCmd
