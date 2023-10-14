package ai.starlake.schema.generator

import ai.starlake.utils.CliConfig
import scopt.OParser

case class TableDependenciesConfig(
  includeAllAttributes: Boolean = true,
  related: Boolean = false,
  outputFile: Option[String] = None,
  tables: Option[Seq[String]] = None,
  reload: Boolean = false,
  all: Boolean = false
)

object TableDependenciesConfig extends CliConfig[TableDependenciesConfig] {
  val command = "table-dependencies"

  val parser: OParser[Unit, TableDependenciesConfig] = {
    val builder = OParser.builder[TableDependenciesConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note("Generate GraphViz files from Domain / Schema YAML files"),
      opt[String]("output")
        .action((x, c) => c.copy(outputFile = Some(x)))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      opt[Unit]("all-attrs")
        .action((x, c) => c.copy(includeAllAttributes = true))
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
      opt[Unit]("related")
        .action((x, c) => c.copy(related = true))
        .optional()
        .text(
          "Should we include only entities with relations to others ? false by default"
        ),
      opt[Seq[String]]("tables")
        .action((x, c) => c.copy(tables = Some(x)))
        .optional()
        .text(
          "Which tables should we include in the dot file ?"
        ),
      opt[Unit]("all")
        .action { (x, c) =>
          c.copy(all = true)
        }
        .optional()
        .text(
          "Include all tables in the dot file ? None by default"
        )
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[TableDependenciesConfig] =
    OParser.parse(parser, args, TableDependenciesConfig())
}
