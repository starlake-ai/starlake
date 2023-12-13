package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import better.files.File
import scopt.OParser

import scala.util.Try

object AutoTaskDependenciesCmd extends Cmd[AutoTaskDependenciesConfig] {

  val command = "lineage"

  val parser: OParser[Unit, AutoTaskDependenciesConfig] = {
    val builder = OParser.builder[AutoTaskDependenciesConfig]
    import builder._
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
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
        .action((_, c) => c.copy(reload = true))
        .optional()
        .text(
          "Should we reload the domains first ?"
        ),
      opt[Unit]("viz")
        .action((_, c) => c.copy(viz = true))
        .optional()
        .text("Should we generate a dot file ?"),
      opt[Unit]("svg")
        .action((_, c) => c.copy(svg = true))
        .optional()
        .text(
          "Should we generate SVG files ?"
        ),
      opt[Unit]("png")
        .action((_, c) => c.copy(png = true))
        .optional()
        .text(
          "Should we generate PNG files ?"
        ),
      opt[Unit]("print")
        .action((_, c) => c.copy(print = true))
        .optional()
        .text("Print dependencies as text"),
      opt[Seq[String]]("objects")
        .action((x, c) => c.copy(objects = x))
        .optional()
        .text("comma separated list of objects to display: task, table, view, unknown"),
      opt[Unit]("all")
        .action { (_, c) =>
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
    OParser.parse(parser, args, AutoTaskDependenciesConfig(), setup)

  override def run(config: AutoTaskDependenciesConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Try {
      val autoTaskDependencies =
        new AutoTaskDependencies(settings, schemaHandler, settings.storageHandler())
      val allDependencies: List[DependencyContext] = autoTaskDependencies.tasks(config)
      if (config.print) autoTaskDependencies.jobsDependencyTree(allDependencies, config)
      if (config.viz) autoTaskDependencies.jobAsDot(allDependencies, config)
    }.map(_ => JobResult.empty)
  }
}
