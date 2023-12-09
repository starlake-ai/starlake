package ai.starlake.schema

import ai.starlake.utils.CliConfig
import scopt.OParser

case class ProjectCompareConfig(
  project1: String = "",
  project2: String = "",
  template: Option[String] = None,
  output: Option[String] = None
)

object ProjectCompareConfig extends CliConfig[ProjectCompareConfig] {
  val command = "compare"

  val parser: OParser[Unit, ProjectCompareConfig] = {
    val builder = OParser.builder[ProjectCompareConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[String]("project1")
        .action { (x, c) => c.copy(project1 = x) }
        .required()
        .text("old project metadata path"),
      opt[String]("project2")
        .action { (x, c) => c.copy(project2 = x) }
        .required()
        .text("new project metadata path"),
      opt[String]("template")
        .action { (x, c) => c.copy(template = Some(x)) }
        .optional()
        .text("SSP / Mustache Template path"),
      opt[String]("output")
        .action { (x, c) => c.copy(output = Some(x)) }
        .optional()
        .text("Output path")
    )
  }

  def parse(args: Seq[String]): Option[ProjectCompareConfig] =
    OParser.parse(parser, args, ProjectCompareConfig(), setup)
}
