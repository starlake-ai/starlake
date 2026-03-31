package ai.starlake.job.ingest

import ai.starlake.job.metrics._
import ai.starlake.job.sink.bigquery._
import ai.starlake.schema.model.{BigQuerySink, JdbcSink}
import ai.starlake.utils.{JobResult, SparkJobResult}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Success, Try}

/** Expectations and metrics concerns for ingestion jobs. */
trait IngestionExpectations { self: IngestionJob =>

  protected def runExpectations(): Try[JobResult] = {
    mergedMetadata.getSink() match {
      case _: BigQuerySink =>
        val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
          settings.appConfig.audit.getDatabase(),
          settings.appConfig.audit.getDomain(),
          "expectations",
          mergedMetadata
            .getSinkConnection()
            .options
            .get("projectId")
            .orElse(settings.appConfig.getDefaultDatabase())
        )
        runBigQueryExpectations(bqNativeJob(tableId, ""))
      case _: JdbcSink =>
        val options = mergedMetadata.getSinkConnection().withAccessToken(accessToken).options
        runJdbcExpectations(options)
      case _ =>
        runSparkExpectations(session)
    }
  }

  private def runExpectationsWithHandler(handler: ExpectationAssertionHandler): Try[JobResult] = {
    if (settings.appConfig.expectations.active) {
      new ExpectationJob(
        Option(applicationId()),
        schemaHandler.getDatabase(this.domain),
        this.domain.finalName,
        this.schema.finalName,
        this.schema.expectations,
        storageHandler,
        schemaHandler,
        handler,
        false
      ).run()
    } else {
      Success(SparkJobResult(None, None))
    }
  }

  private def runJdbcExpectations(jdbcOptions: Map[String, String]): Try[JobResult] =
    runExpectationsWithHandler(new JdbcExpectationAssertionHandler(jdbcOptions))

  private def runSparkExpectations(session: SparkSession): Try[JobResult] =
    runExpectationsWithHandler(new SparkExpectationAssertionHandler(session))

  def runBigQueryExpectations(job: BigQueryNativeJob): Try[JobResult] =
    runExpectationsWithHandler(new BigQueryExpectationAssertionHandler(job))

  protected def runMetrics(acceptedDF: DataFrame): Unit = {
    if (settings.appConfig.metrics.active) {
      new MetricsJob(
        Option(applicationId()),
        this.domain,
        this.schema,
        this.storageHandler,
        this.schemaHandler
      )
        .run(acceptedDF, System.currentTimeMillis())
    }
  }
}