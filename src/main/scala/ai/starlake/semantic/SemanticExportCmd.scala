package ai.starlake.semantic

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

/** Command to export semantic models to an interchange format.
  *
  * Usage: starlake semantic-export [options]
  */
object SemanticExportCmd extends Cmd[SemanticExportConfig] {

  val command = "semantic-export"

  override def pageDescription: String =
    "Export semantic models from metadata/semantic to a vendor-neutral interchange format (Apache Ossie)."
  override def pageKeywords: Seq[String] =
    Seq(
      "starlake semantic-export",
      "semantic model",
      "semantic layer",
      "apache ossie",
      "open semantic interchange",
      "BI",
      "AI agents"
    )

  val parser: OParser[Unit, SemanticExportConfig] = {
    val builder = OParser.builder[SemanticExportConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """
          |Export the semantic models stored in metadata/semantic/ to a vendor-neutral
          |interchange format. Currently supported format: ossie (Apache Ossie, incubating,
          |formerly Open Semantic Interchange).
          |
          |Fields, primary keys, relationships, and metrics are mapped to their Ossie
          |equivalents; Starlake-specific attributes with no Ossie counterpart (filters,
          |sample values, verified query SQL, join types...) are preserved in
          |custom_extensions blocks under the STARLAKE vendor name so no information is lost.
          |
          |example: starlake semantic-export
          |         --format ossie
          |         --model ecommerce_analytics
          |         --output /tmp/ossie-models""".stripMargin
      ),
      builder
        .opt[String]("format")
        .action((x, c) => c.copy(format = x))
        .validate(x =>
          if (x == "ossie") builder.success
          else builder.failure(s"Unsupported format '$x'. Supported formats: ossie")
        )
        .text("Target interchange format. Only 'ossie' is supported for now (default)")
        .optional(),
      builder
        .opt[String]("model")
        .action((x, c) => c.copy(model = Some(x)))
        .text(
          "Name of a single semantic model to export (model 'name' field or file basename). All models by default"
        )
        .optional(),
      builder
        .opt[String]("output")
        .action((x, c) => c.copy(output = Some(x)))
        .text(
          "Output directory. Defaults to metadata/semantic/export/ with one subfolder per format"
        )
        .optional(),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  def parse(args: Seq[String]): Option[SemanticExportConfig] =
    OParser.parse(parser, args, SemanticExportConfig(), setup)

  override def run(config: SemanticExportConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    new SemanticExportJob(config).run()
}
