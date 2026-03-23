package ai.starlake.job.sink.kafka

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JdbcJobResult, JobBase, JobResult}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Success, Try}

/** Kafka load job. TODO: Rewrite to use DuckDB + Kafka Java client for producing/consuming.
  */
class KafkaJob(
  kafkaJobConfig: KafkaJobConfig,
  schemaHandler: SchemaHandler
)(implicit val settings: Settings)
    extends JobBase
    with LazyLogging {

  override def name: String = s"KafkaJob"

  override def run(): Try[JobResult] = {
    logger.warn(
      "KafkaJob is not yet implemented for DuckDB. Kafka operations require migration to pure Kafka Java client."
    )
    Success(JdbcJobResult(Nil))
  }
}
