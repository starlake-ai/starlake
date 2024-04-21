package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.{Success, Try}

trait TransformTestCmd extends Cmd[TransformTestConfig] {

  def command = "test"

  val parser: OParser[Unit, TransformTestConfig] = {
    val builder = OParser.builder[TransformTestConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("accessToken")
        .action((x, c) => c.copy(accessToken = Some(x)))
        .text(s"Access token to use for authentication")
        .optional()
    )
  }

  def parse(args: Seq[String]): Option[TransformTestConfig] = {
    OParser.parse(parser, args, TransformTestConfig(), setup)
  }

  override def run(config: TransformTestConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    workflow(schemaHandler).test(config)
    Success(JobResult.empty)
  }
}

object TransformTestCmd extends TransformTestCmd
