package ai.starlake.job.metrics

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{DomainInfo, SchemaInfo}
import ai.starlake.utils.{JdbcJobResult, JobBase, JobResult}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Success, Try}

/** Metrics computation job. TODO: Rewrite for DuckDB SQL in Phase 4.
  */
class MetricsJob(
  appId: Option[String],
  domain: DomainInfo,
  schema: SchemaInfo,
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler
)(implicit val settings: Settings)
    extends JobBase
    with LazyLogging {

  override def name: String = "Compute metrics job"

  override def applicationId(): String = appId.getOrElse(super.applicationId())

  def run(): Try[JobResult] = {
    logger.warn("MetricsJob is not yet implemented for DuckDB. Skipping metrics computation.")
    Success(JdbcJobResult(Nil))
  }
}
