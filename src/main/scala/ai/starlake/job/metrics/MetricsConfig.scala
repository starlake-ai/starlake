package ai.starlake.job.metrics

import ai.starlake.schema.model.Stage
import ai.starlake.schema.model.Stage
import ai.starlake.utils.CliConfig
import scopt.OParser

case class MetricsConfig(domain: String = "", schema: String = "", stage: Option[Stage] = None)

object MetricsConfig extends CliConfig[MetricsConfig] {
  val command = "metrics"
  val parser: OParser[Unit, MetricsConfig] = {
    val builder = OParser.builder[MetricsConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[String]("domain")
        .action((x, c) => c.copy(domain = x))
        .required()
        .text("Domain Name"),
      opt[String]("schema")
        .action((x, c) => c.copy(schema = x))
        .required()
        .text("Schema Name"),
      opt[String]("stage")
        .action((x, c) => c.copy(stage = Some(Stage.fromString(x))))
        .optional()
        .text("Stage (UNIT or GLOBAL)")
    )
  }

  /** Function to parse command line arguments (domain and schema).
    *
    * @param args
    *   : Command line parameters
    * @return
    *   : an Option of MetricConfing with the parsed domain and schema names.
    */
  def parse(args: Seq[String]): Option[MetricsConfig] = {
    OParser.parse(parser, args, MetricsConfig())
  }
}
