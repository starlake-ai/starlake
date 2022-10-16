package ai.starlake.schema.generator

import ai.starlake.utils.CliConfig
import scopt.OParser

case class Yml2GraphVizConfig(
  includeAllAttributes: Option[Boolean] = Some(true),
  acl: Option[Boolean] = Some(true),
  output: Option[String] = None,
  aclOutput: Option[String] = None,
  reload: Boolean = false
)

object Yml2GraphVizConfig extends CliConfig[Yml2GraphVizConfig] {
  val command = "yml2gv"

  val parser: OParser[Unit, Yml2GraphVizConfig] = {
    val builder = OParser.builder[Yml2GraphVizConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note("Generate GraphViz files from Domain / Schema YAML files"),
      opt[String]("output")
        .action((x, c) => c.copy(output = Some(x)))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      opt[String]("acl-output")
        .action((x, c) => c.copy(aclOutput = Some(x)))
        .optional()
        .text("Where to save the ACL /RLS generated dot file ? Output to the console by default"),
      opt[Boolean]("all")
        .action((x, c) => c.copy(includeAllAttributes = Some(x)))
        .optional()
        .text(
          "Should we include all attributes in the dot file or only the primary and foreign keys ? true by default"
        ),
      opt[Unit]("reload")
        .action((x, c) => c.copy(reload = true))
        .optional()
        .text(
          "Should we reload the domains first ?"
        ),
      opt[Boolean]("acl")
        .action((x, c) => c.copy(acl = Some(x)))
        .optional()
        .text(
          "Should we include all attributes in the dot file or only the primary and foreign keys ? true by default"
        )
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[Yml2GraphVizConfig] =
    OParser.parse(parser, args, Yml2GraphVizConfig())
}
