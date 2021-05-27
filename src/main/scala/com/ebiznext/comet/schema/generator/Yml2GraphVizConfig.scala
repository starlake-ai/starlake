package com.ebiznext.comet.schema.generator

import com.ebiznext.comet.utils.CliConfig
import scopt.OParser

case class Yml2GraphVizConfig(
  includeAllAttributes: Option[Boolean] = Some(true),
  output: Option[String] = None
)

object Yml2GraphVizConfig extends CliConfig[Yml2GraphVizConfig] {

  val parser: OParser[Unit, Yml2GraphVizConfig] = {
    val builder = OParser.builder[Yml2GraphVizConfig]
    import builder._
    OParser.sequence(
      programName("comet yml2gv"),
      head("comet", "yml2gv", "[options]"),
      note("Generate GraphViz files from Domain / Schema YAML files"),
      opt[String]("output")
        .action((x, c) => c.copy(output = Some(x)))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      opt[Boolean]("all")
        .action((x, c) => c.copy(includeAllAttributes = Some(x)))
        .optional()
        .text(
          "Should we include all attributes in the dot file or only the primary and foreign keys ? true by default"
        )
    )
  }

  /** @param args args list passed from command line
    * @return Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[Yml2GraphVizConfig] =
    OParser.parse(parser, args, Yml2GraphVizConfig())
}
