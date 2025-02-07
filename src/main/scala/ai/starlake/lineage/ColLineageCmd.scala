package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import better.files.File
import scopt.OParser

import scala.util.Try

object ColLineageCmd extends Cmd[ColLineageConfig] {

  val command = "col-lineage"

  val parser: OParser[Unit, ColLineageConfig] = {
    val builder = OParser.builder[ColLineageConfig]
    import builder._
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      note("Build lineage"),
      builder
        .opt[String]("output")
        .action((x, c) => c.copy(outputFile = Some(File(x))))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      opt[String]("task")
        .action((x, c) => c.copy(task = x))
        .required()
        .text("task name to buidl lineage for")
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[ColLineageConfig] =
    OParser.parse(parser, args, ColLineageConfig(""), setup)

  override def run(config: ColLineageConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Try {
      val colLineage =
        new ColLineage(settings, schemaHandler)
      colLineage.colLineage(config)

    }.map(_ => JobResult.empty)
  }
}
