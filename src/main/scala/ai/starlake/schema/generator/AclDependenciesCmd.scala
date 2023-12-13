package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import better.files.File
import scopt.OParser

import scala.util.Try

object AclDependenciesCmd extends Cmd[AclDependenciesConfig] {

  val command = "acl-dependencies"

  val parser: OParser[Unit, AclDependenciesConfig] = {
    val builder = OParser.builder[AclDependenciesConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note("Generate GraphViz files from Domain / Schema YAML files"),
      builder
        .opt[String]("output")
        .action((x, c) => c.copy(outputFile = Some(File(x))))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      builder
        .opt[Seq[String]]("grantees")
        .action { (x, c) =>
          c.copy(grantees = x.toList)
        }
        .optional()
        .text(
          "Which users should we include in the dot file ? All by default"
        ),
      builder
        .opt[Unit]("reload")
        .action((_, c) => c.copy(reload = true))
        .optional()
        .text(
          "Should we reload the domains first ?"
        ),
      builder
        .opt[Unit]("svg")
        .action((_, c) => c.copy(svg = true))
        .optional()
        .text(
          "Should we generate SVG files ?"
        ),
      builder
        .opt[Unit]("png")
        .action((_, c) => c.copy(png = true))
        .optional()
        .text(
          "Should we generate PNG files ?"
        ),
      builder
        .opt[Seq[String]]("tables")
        .action { (x, c) =>
          c.copy(tables = x.toList)
        }
        .optional()
        .text(
          "Which tables should we include in the dot file ? All by default"
        ),
      builder
        .opt[Unit]("all")
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
    OParser.parse(parser, args, AclDependenciesConfig(), setup)

  override def run(config: AclDependenciesConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    Try(new AclDependencies(schemaHandler).aclsAsDotFile(config)).map(_ => JobResult.empty)
}
