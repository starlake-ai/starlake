package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

object AclCmd extends Cmd[AclConfig] {

  val command = "acl"

  override def pageDescription: String =
    "Export all ACL and Row Level Security entries to a markdown file. Supports local and cloud filesystem paths."
  override def pageKeywords: Seq[String] =
    Seq("starlake acl", "ACL export", "row level security", "access control", "RLS")

  val parser: OParser[Unit, AclConfig] = {
    val builder = OParser.builder[AclConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        "Export all ACL and Row Level Security entries from domain and task definitions to a markdown file."
      ),
      builder
        .opt[Unit]("export")
        .action((_, c) => c.copy(`export` = true))
        .required()
        .text("Export ACL and RLS entries to a file"),
      builder
        .opt[String]("outputPath")
        .action((x, c) => c.copy(outputPath = x))
        .required()
        .text(
          "Path to output file (local or cloud filesystem, e.g. /path/to/acl.md or gs://bucket/acl.md)"
        ),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  def parse(args: Seq[String]): Option[AclConfig] =
    OParser.parse(parser, args, AclConfig(), setup)

  override def run(config: AclConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = Try {
    if (config.`export`) {
      new AclExport(schemaHandler).exportToFile(config)
    }
    JobResult.empty
  }
}
