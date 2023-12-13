package ai.starlake.job.metrics

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

object MetricsCmd extends Cmd[MetricsConfig] {

  val command = "metrics"

  val parser: OParser[Unit, MetricsConfig] = {
    val builder = OParser.builder[MetricsConfig]
    import builder._
    OParser.sequence(
      programName(s"$shell $command"),
      head(shell, command, "[options]"),
      note(""),
      opt[String]("domain")
        .action((x, c) => c.copy(domain = x))
        .required()
        .text("Domain Name"),
      opt[String]("schema")
        .action((x, c) => c.copy(schema = x))
        .required()
        .text("Schema Name"),
      opt[Map[String, String]]("authInfo")
        .action((x, c) => c.copy(authInfo = x))
        .optional()
        .text("Auth Info.  Google Cloud use: gcpProjectId and gcpSAJsonKey")
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
    OParser.parse(parser, args, MetricsConfig(), setup)
  }

  override def run(config: MetricsConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).metric(config)
}
