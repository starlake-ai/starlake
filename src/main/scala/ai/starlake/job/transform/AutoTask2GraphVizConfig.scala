package ai.starlake.job.transform

import ai.starlake.utils.CliConfig
import scopt.OParser

case class AutoTask2GraphVizConfig(
  output: Option[String] = None,
  job: Option[String] = None,
  reload: Boolean = false
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
      opt[String]("output")
        .action((x, c) => c.copy(output = Some(x)))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      opt[Option[String]]("jobs")
        .action((x, c) => c.copy(job = x))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      opt[Unit]("reload")
        .action((x, c) => c.copy(reload = true))
        .optional()
        .text(
          "Should we reload the domains first ?"
        )
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
