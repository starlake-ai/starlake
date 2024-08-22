package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.ConnectionType
import ai.starlake.utils.{JobResult, SparkJob}

import scala.util.Try

object SchemaExtractor {
  def extractSchemas(
    connectionName: String
  )(implicit settings: Settings): Try[List[(String, List[String])]] = {
    val connection = settings.appConfig.connections(connectionName)
    val connType = connection.getType()
    connType match {
      case ConnectionType.JDBC =>
        val result = JdbcDbUtils.extractSchemasAndTables(connection)
        result
      case ConnectionType.BQ =>
        val extractor = new ExtractBigQuerySchema(
          BigQueryTablesConfig(connectionRef = Some(connectionName))
        )
        val schemaHandler = new SchemaHandler(settings.storageHandler())
        val result = extractor.extractSchemasAndTables(schemaHandler)
        result

      case ConnectionType.FS =>
        val job = new SparkExtractorJob()
        job.schemasAndTables()
      case _ =>
        Try {
          throw new IllegalArgumentException(s"Unsupported connection type: $connType")
        }

    }
  }

  class SparkExtractorJob(implicit s: Settings) extends SparkJob {
    override def name: String = "extractor"

    override implicit def settings: Settings = s

    override def run(): Try[JobResult] = Try {
      JobResult.empty
    }

    def schemasAndTables(): Try[List[(String, List[String])]] = Try {
      session
        .sql("show databases")
        .collect()
        .map { row =>
          val dbName = row.getString(0)
          val tables =
            session.sql(s"show tables in $dbName").collect().map(_.getString(1)).toList
          dbName -> tables
        }
        .toList
    }
  }
}
