package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.extract.BigQueryTableInfo.sink
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult

import scala.util.Try

/** Command to get table information from BigQuery.
  *
  * Usage: starlake bq-info [options]
  */
object BigQueryTableInfoCmd extends TablesExtractCmd {

  override def command: String = "bq-info"

  override def pageDescription: String =
    "Retrieve metadata and information about BigQuery tables and datasets, with options to persist results or filter by table."
  override def pageKeywords: Seq[String] =
    Seq("starlake bq-info", "BigQuery", "table metadata", "dataset info")

  override def run(config: TablesExtractConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Try(sink(config)).map(_ => JobResult.empty)
  }
}
