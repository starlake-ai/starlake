package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.tests.StarlakeTestConfig
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.{Success, Try}

trait StarlakeTestCmd extends Cmd[StarlakeTestConfig] {

  def command = "test"

  val parser: OParser[Unit, StarlakeTestConfig] = {
    val builder = OParser.builder[StarlakeTestConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[Unit]("load")
        .action((x, c) => c.copy(load = true))
        .text(s"Test load tasks only")
        .optional(),
      builder
        .opt[Unit]("transform")
        .action((x, c) => c.copy(transform = true))
        .text(s"Test transform tasks only")
        .optional(),
      builder
        .opt[Option[String]]("name")
        .action { (x, c) =>
          val splitted = x.map(_.split('.'))
          if (splitted.exists(_.length != 3)) {
            throw new IllegalArgumentException(
              "Invalid test format. Use 'domainName.taskName.testName' or 'domainName.tableName.testName'"
            )
          }
          c.copy(name = x)
        }
        .text(s"Test this test only")
        .optional(),
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
    Success(workflow(schemaHandler).test(config))
  }
}

object StarlakeTestCmd extends StarlakeTestCmd
