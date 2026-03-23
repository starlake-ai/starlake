package ai.starlake.job.sink.jdbc

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.utils.{JdbcJobResult, JobBase, JobResult}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Success, Try}

/** JDBC writer job. TODO: Rewrite to use DuckDB COPY or direct JDBC INSERT.
  */
class SparkJdbcWriter(
  cliConfig: JdbcConnectionLoadConfig
)(implicit val settings: Settings)
    extends JobBase
    with LazyLogging {

  override def name: String = s"cnxload-JDBC-${cliConfig.outputDomainAndTableName}"

  override def run(): Try[JobResult] = {
    val jdbcOptions =
      JdbcDbUtils.jdbcOptions(cliConfig.options, cliConfig.format, cliConfig.accessToken)
    logger.warn(
      "SparkJdbcWriter is not yet fully implemented for DuckDB. " +
      s"Loading to ${cliConfig.outputDomainAndTableName} requires migration to DuckDB COPY."
    )
    Success(JdbcJobResult(Nil))
  }
}
