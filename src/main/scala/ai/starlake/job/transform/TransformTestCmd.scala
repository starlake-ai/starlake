package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.tests.StarlakeTestConfig
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.{Success, Try}

trait TransformTestCmd extends Cmd[StarlakeTestConfig] {

  def command = "test"

  val parser: OParser[Unit, StarlakeTestConfig] = {
    val builder = OParser.builder[StarlakeTestConfig]
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

  def parse(args: Seq[String]): Option[StarlakeTestConfig] = {
    OParser.parse(parser, args, StarlakeTestConfig(), setup)
  }

  override def run(config: StarlakeTestConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    workflow(schemaHandler).test(config)
    Success(JobResult.empty)
  }
}

object TransformTestCmd extends TransformTestCmd
