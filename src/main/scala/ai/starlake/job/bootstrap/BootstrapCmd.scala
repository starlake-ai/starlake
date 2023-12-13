package ai.starlake.job.bootstrap

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.{Success, Try}

object BootstrapCmd extends Cmd[BootstrapConfig] {

  override val command: String = "bootstrap"

  val parser: OParser[Unit, BootstrapConfig] = {
    val builder = OParser.builder[BootstrapConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """
          |Create a new project optionally based on a specific template eq. quickstart / userguide
          |""".stripMargin
      ),
      builder
        .opt[String]("template")
        .action((x, c) => c.copy(template = Some(x)))
        .text("Template to use to bootstrap project")
        .optional()
    )
  }

  def parse(args: Seq[String]): Option[BootstrapConfig] =
    OParser.parse(parser, args, BootstrapConfig(), setup)

  override def run(config: BootstrapConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Bootstrap.bootstrap(config.template)
    Success(JobResult.empty)
  }
}
