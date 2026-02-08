package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult

import scala.util.Try

/** Command to apply security policies (RLS/CLS).
  *
  * Usage: starlake secure [options]
  */
object SecureCmd extends LoadCmd {
  override def command = "secure"

  override def run(config: LoadConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).secure(config).map(_ => JobResult.empty)
}
