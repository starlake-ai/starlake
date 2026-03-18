package ai.starlake.workflow

import ai.starlake.config.Settings
import ai.starlake.job.sink.es.{ESLoadConfig, ESLoadJob}
import ai.starlake.job.sink.jdbc.{JdbcConnectionLoadConfig, SparkJdbcWriter}
import ai.starlake.job.sink.kafka.{KafkaJob, KafkaJobConfig}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.utils.JobResult
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

trait SinkWorkflow extends LazyLogging {
  this: IngestionWorkflow =>

  protected def storageHandler: StorageHandler
  protected def schemaHandler: SchemaHandler

  def esLoad(config: ESLoadConfig): Try[JobResult] = {
    val res = new ESLoadJob(config, storageHandler, schemaHandler).run()
    Utils.logFailure(res, logger)
  }

  def kafkaload(config: KafkaJobConfig): Try[JobResult] = {
    val res = new KafkaJob(config, schemaHandler).run()
    Utils.logFailure(res, logger)
  }

  def jdbcload(config: JdbcConnectionLoadConfig): Try[JobResult] = {
    val loadJob = new SparkJdbcWriter(config)
    val res = loadJob.run()
    Utils.logFailure(res, logger)
  }
}
