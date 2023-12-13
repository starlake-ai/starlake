package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.extract.BigQueryTableInfo.sink
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult

import scala.util.Try

object BigQueryTableInfoCmd extends BigQueryTablesCmd {

  override def command: String = "bq-info"

  override def run(config: BigQueryTablesConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Try(sink(config)).map(_ => JobResult.empty)
  }
}
