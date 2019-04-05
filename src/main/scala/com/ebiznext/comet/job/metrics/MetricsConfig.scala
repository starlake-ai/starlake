package com.ebiznext.comet.job.metrics

import com.ebiznext.comet.config.Settings
import org.apache.hadoop.fs.Path
import scopt.OParser

case class MetricsConfig(
                          domain: String = "",
                          schema: String = ""
                        ) {

  /** Function to build path of data to compute metrics on
    * using the attribute domain and schema and ingested files path.
    *
    * @return : the path of data to compute metrics on
    */
  def getDataset(): Path = {
    new Path(s"${Settings.comet.datasets}/${Settings.comet.area.accepted}/$domain/$schema")
  }

}

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
          .text("Schema Name")
      )
    }
    OParser.parse(parser, args, MetricsConfig())
  }
}
