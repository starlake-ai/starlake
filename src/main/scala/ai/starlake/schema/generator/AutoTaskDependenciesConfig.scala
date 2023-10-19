package ai.starlake.schema.generator

import ai.starlake.utils.CliConfig
import better.files.File
import scopt.OParser

case class AutoTaskDependenciesConfig(
  outputFile: Option[File] = None,
  tasks: Option[Seq[String]] = None,
  reload: Boolean = false,
  objects: Seq[String] = Seq("task", "table"),
  viz: Boolean = false,
  print: Boolean = false,
  svg: Boolean = false,
  png: Boolean = false,
  all: Boolean = false
)

object AutoTaskDependenciesConfig extends CliConfig[AutoTaskDependenciesConfig] {
  val command = "task-dependencies"

  val parser: OParser[Unit, AutoTaskDependenciesConfig] = {
    val builder = OParser.builder[AutoTaskDependenciesConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note("Generate Task dependencies graph"),
      opt[String]("output")
        .action((x, c) => c.copy(outputFile = Some(File(x))))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      opt[Seq[String]]("tasks")
        .action((x, c) => c.copy(tasks = Some(x)))
        .optional()
        .text("Compute dependencies of this job only. If not specified, compute all jobs."),
      opt[Unit]("reload")
        .action((x, c) => c.copy(reload = true))
        .optional()
        .text(
          "Should we reload the domains first ?"
        ),
      opt[Unit]("viz")
        .action((x, c) => c.copy(viz = true))
        .optional()
        .text("Should we generate a dot file ?"),
      opt[Unit]("svg")
        .action((x, c) => c.copy(svg = true))
        .optional()
        .text(
          "Should we generate SVG files ?"
        ),
      opt[Unit]("png")
        .action((x, c) => c.copy(png = true))
        .optional()
        .text(
          "Should we generate PNG files ?"
        ),
      opt[Unit]("print")
        .action((x, c) => c.copy(print = true))
        .optional()
        .text("Print dependencies as text"),
      opt[Seq[String]]("objects")
        .action((x, c) => c.copy(objects = x))
        .optional()
        .text("comma separated list of objects to display: task, table, view, unknown"),
      opt[Unit]("all")
        .action { (x, c) =>
          c.copy(all = true)
        }
        .optional()
        .text(
          "Include all tasks  in the dot file ? None by default"
        )
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[AutoTaskDependenciesConfig] =
    OParser.parse(parser, args, AutoTaskDependenciesConfig())
}
