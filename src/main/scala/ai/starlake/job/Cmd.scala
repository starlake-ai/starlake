package ai.starlake.job

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{CliConfig, JobResult}
import ai.starlake.workflow.IngestionWorkflow

import scala.util.{Failure, Try}

trait Cmd[T] extends CliConfig[T] {

  val shell: String = Main.shell

  final def run(
    args: Seq[String],
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Try[JobResult] = {
    parse(args) match {
      case Some(config) => run(config, schemaHandler)
      case None =>
        Failure(new IllegalArgumentException(usage()))
    }
  }

  def run(config: T, schemaHandler: SchemaHandler)(implicit settings: Settings): Try[JobResult]

  def workflow(schemaHandler: SchemaHandler)(implicit settings: Settings): IngestionWorkflow =
    new IngestionWorkflow(settings.storageHandler(), schemaHandler)

}
