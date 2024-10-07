package ai.starlake.tests

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
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
        .action((_, c) => c.copy(load = true))
        .text(s"Test load tasks only")
        .optional(),
      builder
        .opt[Unit]("transform")
        .action((_, c) => c.copy(transform = true))
        .text(s"Test transform tasks only")
        .optional(),
      builder
        .opt[Option[String]]("domain")
        .action { (x, c) => c.copy(domain = x) }
        .text(s"Test this domain only")
        .optional(),
      builder
        .opt[Option[String]]("table")
        .action { (x, c) => c.copy(test = x) }
        .text(s"Test this table or task only in the selected domain")
        .optional(),
      builder
        .opt[Option[String]]("test")
        .action { (x, c) => c.copy(test = x) }
        .text(s"Test this test only in the domain and table/task selected")
        .optional(),
      builder
        .opt[Unit]("site")
        .action { (_, c) => c.copy(generate = true) }
        .text(s"Generate the results of the tests as a website")
        .optional(),
      builder
        .opt[Option[String]]("outputDir")
        .action { (x, c) => c.copy(outputDir = x) }
        .text(s"Where to output the tests")
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
