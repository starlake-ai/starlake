package ai.starlake.schema

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

object ProjectCompareCmd extends Cmd[ProjectCompareConfig] {
  val command = "compare"

  val parser: OParser[Unit, ProjectCompareConfig] = {
    val builder = OParser.builder[ProjectCompareConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("project1")
        .action { (x, c) => c.copy(project1 = x) }
        .required()
        .text("old project metadata path"),
      builder
        .opt[String]("project2")
        .action { (x, c) => c.copy(project2 = x) }
        .required()
        .text("new project metadata path"),
      builder
        .opt[String]("template")
        .action { (x, c) => c.copy(template = Some(x)) }
        .optional()
        .text("SSP / Mustache Template path"),
      builder
        .opt[String]("output")
        .action { (x, c) => c.copy(output = Some(x)) }
        .optional()
        .text("Output path")
    )
  }

  def parse(args: Seq[String]): Option[ProjectCompareConfig] =
    OParser.parse(parser, args, ProjectCompareConfig(), setup)

  override def run(config: ProjectCompareConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    Try(ProjectCompare.compare(config)).map(_ => JobResult.empty)
}
