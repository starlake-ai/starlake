package ai.starlake.job.transform

import ai.starlake.utils.CliConfig
import scopt.OParser

case class AutoTask2GraphVizConfig(
  outputDir: Option[String] = None,
  task: Option[String] = None,
  reload: Boolean = false,
  verbose: Boolean = false,
  objects: Seq[String] = Seq("task", "table")
)

object AutoTask2GraphVizConfig extends CliConfig[AutoTask2GraphVizConfig] {
  val command = "jobs2gv"

  val parser: OParser[Unit, AutoTask2GraphVizConfig] = {
    val builder = OParser.builder[AutoTask2GraphVizConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note("Generate GraphViz files from Job YAML files"),
      opt[String]("output-dir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      opt[Option[String]]("task")
        .action((x, c) => c.copy(task = x))
        .optional()
        .text("Compute dependencies of this job only. If not specified, compute all jobs."),
      opt[Unit]("reload")
        .action((x, c) => c.copy(reload = true))
        .optional()
        .text(
          "Should we reload the domains first ?"
        ),
      opt[Unit]("verbose")
        .action((x, c) => c.copy(verbose = true))
        .optional()
        .text("Should we generate one graph per job ?"),
      opt[Seq[String]]("objects")
        .action((x, c) => c.copy(objects = x))
        .optional()
        .text("comma separated list of objects to display: task, table, view, unknown")
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[AutoTask2GraphVizConfig] =
    OParser.parse(parser, args, AutoTask2GraphVizConfig())
}
