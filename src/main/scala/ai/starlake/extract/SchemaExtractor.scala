package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Attribute, ConnectionType, Domain, Schema}
import ai.starlake.utils.{JobResult, SparkJob}

import java.util.regex.Pattern
import scala.util.{Success, Try}

object SchemaExtractor {
  def extractSchemaNames(
    connectionName: String,
    accessToken: Option[String],
    tables: Map[String, List[String]] = Map.empty
  )(implicit settings: Settings): Try[List[(String, List[String])]] = {
    val connection = settings.appConfig.connections(connectionName)
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
        val schemaHandler = new SchemaHandler(settings.storageHandler())
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

  class SparkExtractorJob(domainAndTablesNames: List[(String, List[String])] = Nil)(implicit
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
      tableNames.map { tableName =>
        session
          .sql(s"describe $tableName")
          .collect()
          .map { row =>
            val colName = row.getString(0)
            val colType = row.getString(1)
            colName -> colType
          }
          .toList
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
      domains
    }

  }
}
