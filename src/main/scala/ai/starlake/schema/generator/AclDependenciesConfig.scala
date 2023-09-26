package ai.starlake.schema.generator

import ai.starlake.utils.CliConfig
import scopt.OParser

case class AclDependenciesConfig(
  grantees: List[String] = Nil,
  outputFile: Option[String] = None,
  reload: Boolean = false
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
