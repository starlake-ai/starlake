package ai.starlake.console

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

object ConsoleCmd extends Cmd[ConsoleConfig] {
  override def command: String = "console"

  val parser: OParser[Unit, ConsoleConfig] = {
    val builder = OParser.builder[ConsoleConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(options = c.options ++ x))
        .unbounded()
        .text("Options(ignored)")
    )
  }

  override def parse(args: Seq[String]): Option[ConsoleConfig] =
    OParser.parse(parser, args, ConsoleConfig(), setup)

  override def run(config: ConsoleConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Console.console(Console.handler)
    Try(JobResult.empty)
  }
}
