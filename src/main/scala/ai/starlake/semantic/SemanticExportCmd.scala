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
    "Export semantic models from metadata/semantic to a vendor-neutral interchange format (Apache Ossie) or a LookML project."
  override def pageKeywords: Seq[String] =
    Seq(
      "starlake semantic-export",
      "semantic model",
      "semantic layer",
      "apache ossie",
      "lookml",
      "looker",
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
          |Export the semantic models stored in metadata/semantic/ to another semantic
          |format. Supported formats: ossie (Apache Ossie, incubating, formerly Open
          |Semantic Interchange) and lookml (a Looker project: one view file per table
          |plus a model file with explores).
          |
          |For ossie, fields, primary keys, relationships, and metrics are mapped to
          |their Ossie equivalents; Starlake-specific attributes with no Ossie
          |counterpart (filters, sample values, verified query SQL, join types...) are
          |preserved in custom_extensions blocks under the STARLAKE vendor name so no
          |information is lost.
          |
          |For lookml, dimensions, time dimensions, facts and metrics become LookML
          |dimensions, dimension_groups and measures; relationships become explores
          |with joins. --connection sets the Looker connection name in the model file
          |(defaults to the project's connectionRef).
          |
          |example: starlake semantic-export
          |         --format lookml
          |         --model ecommerce_analytics
          |         --connection analytics_wh
          |         --output /tmp/lookml-models""".stripMargin
      ),
      builder
        .opt[String]("format")
        .action((x, c) => c.copy(format = x))
        .validate(x =>
          if (Set("ossie", "lookml").contains(x)) builder.success
          else builder.failure(s"Unsupported format '$x'. Supported formats: ossie, lookml")
        )
        .text("Target format: ossie (default) or lookml")
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
      builder
        .opt[String]("connection")
        .action((x, c) => c.copy(connection = Some(x)))
        .text(
          "lookml only: Looker connection name written to the model file. Defaults to the project's connectionRef"
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
