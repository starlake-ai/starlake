package ai.starlake.extract

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.model.{Attribute, ConnectionType, Domain, Schema}
import ai.starlake.utils.{JobResult, SparkJob}

import java.util.regex.Pattern
import scala.util.{Success, Try}

object SchemaExtractor {
  def extractSchemas(
    connectionName: String,
    accessToken: Option[String],
    domainAndTablesNames: Map[String, List[String]] = Map.empty
  )(implicit settings: Settings) = {
    val connection = settings.appConfig.connections(connectionName).withAccessToken(accessToken)
    val connType = connection.`type`
    val schemaHandler = settings.schemaHandler()
    if (settings.appConfig.autoExportSchema) {
      settings.storageHandler().delete(DatasetArea.external)
      settings.storageHandler().mkdirs(DatasetArea.external)
    }
    connType match {
      case ConnectionType.JDBC | ConnectionType.BQ =>
        val tables = domainAndTablesNames.flatMap { case (key, value) =>
          value.map { table => s"$key.$table" }
        }
        val config = ExtractSchemaConfig(
          external = settings.appConfig.autoExportSchema,
          outputDir = Some(DatasetArea.external.toString),
          tables = tables.toList,
          connectionRef = Some(connectionName),
          accessToken = accessToken
        )
        val result = ExtractSchemaCmd.run(config, schemaHandler)
        result

      case ConnectionType.FS =>
        val job = new SparkExtractorJob(domainAndTablesNames)
        val result = job.schemasAndTables() match {
          case Success(domains) =>
            if (settings.appConfig.autoExportSchema) {
              schemaHandler.saveToExternals(domains)
            }
          case _ => throw new Exception("Failed to extract schemas and tables")
        }
        result
      case _ =>
        throw new IllegalArgumentException(s"Unsupported connection type: $connType")
    }
  }

  def extractSchemaNames(
    connectionName: String,
    accessToken: Option[String],
    tables: Map[String, List[String]] = Map.empty
  )(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Try[List[(String, List[String])]] = {
    val connection = settings.appConfig.connections(connectionName).withAccessToken(accessToken)
    val connType = connection.`type`
    connType match {
      case ConnectionType.JDBC =>
        val result = JdbcDbUtils.extractSchemasAndTableNames(connection)
        result
      case ConnectionType.BQ =>
        val extractor = new ExtractBigQuerySchema(
          BigQueryTablesConfig(
            connectionRef = Some(connectionName),
            accessToken = accessToken,
            tables = tables
          )
        )
        val schemaHandler = settings.schemaHandler()
        val result = extractor.extractSchemasAndTableNames(schemaHandler)
        result

      case ConnectionType.FS =>
        val job = new SparkExtractorJob()
        job.schemasAndTableNames()
      case _ =>
        Try {
          throw new IllegalArgumentException(s"Unsupported connection type: $connType")
        }

    }
  }
}

class SparkExtractorJob(domainAndTablesNames: Map[String, List[String]] = Map.empty)(implicit
  s: Settings
) extends SparkJob {
  override def name: String = "extractor"

  override implicit def settings: Settings = s

  override def run(): Try[JobResult] = Try {
    JobResult.empty
  }

  def schemasAndTableNames(): Try[List[(String, List[String])]] = Try {
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
  def schemasAndTables(
  ): Try[List[Domain]] = Try {
    val tableMap =
      if (domainAndTablesNames.isEmpty) {
        schemasAndTableNames()
      } else {
        Success(domainAndTablesNames)
      }
    val tableNames =
      tableMap match {
        case Success(value) =>
          value.flatMap { case (schema, tables) =>
            tables.map(table => s"$schema.$table")
          }
        case _ => throw new Exception("Failed to extract schemas and tables")
      }

    val domains =
      tableMap match {
        case Success(domainNameMap) =>
          domainNameMap.map { case (schemaName, tables) =>
            val schemas =
              tables.map { table =>
                val cols =
                  session
                    .sql(s"describe $schemaName.$table")
                    .collect()
                    .map { row =>
                      val colName = row.getString(0)
                      val colType = row.getString(1)
                      Attribute(colName, colType)
                    }
                    .toList
                Schema(
                  table,
                  pattern = Pattern.compile(s"$schemaName.$table.*"),
                  attributes = cols
                )
              }
            Domain(schemaName, tables = schemas)
          }
        case _ => throw new Exception("Failed to extract schemas and tables")
      }
    domains.toList
  }
}
