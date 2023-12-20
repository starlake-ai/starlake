package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

object DagGenerateCmd extends Cmd[DagGenerateConfig] {
  val command = "dag-generate"

  val parser: OParser[Unit, DagGenerateConfig] = {
    val builder = OParser.builder[DagGenerateConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .optional()
        .text(
          """Path for saving the resulting DAG file(s).""".stripMargin
        ),
      builder
        .opt[Unit]("clean")
        .action((x, c) => c.copy(clean = true))
        .optional()
        .text(
          """Clean Resulting DAg file output first ?""".stripMargin
        ),
      builder
        .opt[Seq[String]]("tags")
        .action((x, c) => c.copy(tags = x))
        .optional()
        .text(
          """Clean Resulting DAg file output first ?""".stripMargin
        )
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[DagGenerateConfig] =
    OParser.parse(parser, args, DagGenerateConfig(), setup)

  override def run(config: DagGenerateConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    Try(new DagGenerateCommand(schemaHandler).generateDomainDags(config)).map(_ => JobResult.empty)
}
