package ai.starlake.job.infer

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Format, WriteMode}
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

object InferSchemaCmd extends Cmd[InferSchemaConfig] {

  val command = "infer-schema"

  val parser: OParser[Unit, InferSchemaConfig] = {
    val builder = OParser.builder[InferSchemaConfig]
    import builder._
    OParser.sequence(
      programName(s"$shell $command"),
      head(shell, command, "[options]"),
      note(""),
      opt[String]("domain")
        .action((x, c) => c.copy(domainName = x))
        .required()
        .text("Domain Name"),
      opt[String]("table")
        .action((x, c) => c.copy(schemaName = x))
        .required()
        .text("Table Name"),
      opt[String]("input")
        .action((x, c) => c.copy(inputPath = x))
        .required()
        .text("Dataset Input Path"),
      opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .optional()
        .text("Domain YAML Output Path"),
      opt[String]("write")
        .action((x, c) => c.copy(write = Some(WriteMode.fromString(x))))
        .text(s"One of ${WriteMode.writes}")
        .optional(),
      opt[String]("format")
        .action((x, c) => c.copy(format = Some(Format.fromString(x))))
        .optional()
        .text("Force input file format"),
      opt[Unit]("with-header")
        .action((_, c) => c.copy(withHeader = true))
        .optional()
        .text("Does the file contain a header (For CSV files only)")
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class InferSchemaConfig.
    */
  def parse(args: Seq[String]): Option[InferSchemaConfig] =
    OParser.parse(parser, args, InferSchemaConfig(), setup)

  override def run(config: InferSchemaConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).inferSchema(config).map(_ => JobResult.empty)
}
