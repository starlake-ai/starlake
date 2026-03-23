package ai.starlake.job.sink.es

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.utils.{JdbcJobResult, JobBase, JobResult}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Success, Try}

/** Elasticsearch load job. TODO: Rewrite to use DuckDB + ES REST bulk API.
  */
class ESLoadJob(
  cliConfig: ESLoadConfig,
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler
)(implicit val settings: Settings)
    extends JobBase
    with LazyLogging {

  override def name: String = s"Index ${cliConfig.getDataset()}"

  override def run(): Try[JobResult] = {
    logger.warn(
      "ESLoadJob is not yet implemented for DuckDB. Elasticsearch indexing requires migration to REST bulk API."
    )
    Success(JdbcJobResult(Nil))
  }
}
