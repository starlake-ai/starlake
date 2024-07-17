package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
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
      opt[String]("task")
        .action((x, c) => c.copy(task = x))
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
      val autoTaskDependencies =
        new AutoTaskDependencies(settings, schemaHandler, settings.storageHandler())
      autoTaskDependencies.colLinage(config.task)

    }.map(_ => JobResult.empty)
  }
}
