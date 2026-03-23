package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.model.{DomainInfo, SchemaInfo, TableAttribute}
import ai.starlake.utils.{JobBase, JobResult}
import com.typesafe.scalalogging.LazyLogging

import java.util.regex.Pattern
import scala.util.{Success, Try}

class SparkExtractorJob(domainAndTablesNames: Map[String, List[String]] = Map.empty)(implicit
  s: Settings
) extends JobBase
    with LazyLogging {
  override def name: String = "extractor"

  override implicit def settings: Settings = s

  override def run(): Try[JobResult] = Try {
    JobResult.empty
  }

  def schemasAndTableNames(): Try[List[(String, List[String])]] = Try {
    val connectionOptions = settings.appConfig.getDefaultConnection().options
    JdbcDbUtils.withJDBCConnection(None, connectionOptions) { conn =>
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery("SHOW DATABASES")
      val databases = Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList
      rs.close()
      stmt.close()
      databases.map { dbName =>
        val tableStmt = conn.createStatement()
        val tableRs = tableStmt.executeQuery(s"SHOW TABLES IN $dbName")
        val tables =
          Iterator.continually(tableRs).takeWhile(_.next()).map(_.getString(1)).toList
        tableRs.close()
        tableStmt.close()
        dbName -> tables
      }
    }
  }

  def schemasAndTables(): Try[List[DomainInfo]] = Try {
    val tableMap =
      if (domainAndTablesNames.isEmpty) {
        schemasAndTableNames()
      } else {
        Success(domainAndTablesNames)
      }
    val domainNameMap = tableMap match {
      case Success(value) => value
      case _              => throw new Exception("Failed to extract schemas and tables")
    }

    val connectionOptions = settings.appConfig.getDefaultConnection().options
    JdbcDbUtils
      .withJDBCConnection(None, connectionOptions) { conn =>
        domainNameMap.map { case (schemaName, tables) =>
          val schemas = tables.map { table =>
            val stmt = conn.createStatement()
            val rs = stmt.executeQuery(s"DESCRIBE $schemaName.$table")
            val cols = Iterator
              .continually(rs)
              .takeWhile(_.next())
              .map { r =>
                val colName = r.getString(1)
                val colType = r.getString(2)
                TableAttribute(colName, colType)
              }
              .toList
            rs.close()
            stmt.close()
            SchemaInfo(
              table,
              pattern = Pattern.compile(s"$schemaName.$table.*"),
              attributes = cols
            )
          }
          DomainInfo(schemaName, tables = schemas)
        }
      }
      .toList
  }
}
