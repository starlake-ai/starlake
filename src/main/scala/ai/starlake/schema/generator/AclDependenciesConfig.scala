package ai.starlake.schema.generator

import ai.starlake.utils.CliConfig
import scopt.OParser

case class AclDependenciesConfig(
  grantees: List[String] = Nil,
  tables: List[String] = Nil,
  outputFile: Option[String] = None,
  reload: Boolean = false,
  svg: Boolean = false,
  all: Boolean = false
)
object AclDependenciesConfig extends CliConfig[AclDependenciesConfig] {
  val command = "acl-dependencies"
  val parser: OParser[Unit, AclDependenciesConfig] = {
    val builder = OParser.builder[AclDependenciesConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note("Generate GraphViz files from Domain / Schema YAML files"),
      opt[String]("output")
        .action((x, c) => c.copy(outputFile = Some(x)))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      opt[Seq[String]]("grantees")
        .action { (x, c) =>
          c.copy(grantees = x.toList)
        }
        .optional()
        .text(
          "Which users should we include in the dot file ? All by default"
        ),
      opt[Unit]("reload")
        .action((x, c) => c.copy(reload = true))
        .optional()
        .text(
          "Should we reload the domains first ?"
        ),
      opt[Unit]("svg")
        .action((x, c) => c.copy(svg = true))
        .optional()
        .text(
          "Should we generate SVG files ?"
        ),
      opt[Seq[String]]("tables")
        .action { (x, c) =>
          c.copy(tables = x.toList)
        }
        .optional()
        .text(
          "Which tables should we include in the dot file ? All by default"
        ),
      opt[Unit]("all")
        .action { (x, c) =>
          c.copy(all = true)
        }
        .optional()
        .text(
          "Include all ACL in the dot file ? None by default"
        )
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[AclDependenciesConfig] =
    OParser.parse(parser, args, AclDependenciesConfig())
}
