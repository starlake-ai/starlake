package com.ebiznext.comet.job.metrics

import com.ebiznext.comet.schema.model.Stage
import scopt.OParser

case class MetricsConfig(domain: String = "", schema: String = "", stage: Option[Stage] = None)

object MetricsConfig {

  /** Function to parse command line arguments (domain and schema).
    *
    * @param args : Command line parameters
    * @return : an Option of MetricConfing with the parsed domain and schema names.
    */
  def parse(args: Seq[String]): Option[MetricsConfig] = {
    val builder = OParser.builder[MetricsConfig]
    val parser: OParser[Unit, MetricsConfig] = {
      import builder._
      OParser.sequence(
        programName("comet"),
        head("comet", "1.x"),
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
          .text("Stage (UNIT or GLOBAL")
      )
    }
    OParser.parse(parser, args, MetricsConfig())
  }
}
