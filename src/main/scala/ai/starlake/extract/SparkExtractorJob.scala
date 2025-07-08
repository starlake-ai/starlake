package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.model.{Attribute, DomainInfo, SchemaInfo}
import ai.starlake.utils.{JobResult, SparkJob}

import java.util.regex.Pattern
import scala.util.{Success, Try}

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
  ): Try[List[DomainInfo]] = Try {
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
                SchemaInfo(
                  table,
                  pattern = Pattern.compile(s"$schemaName.$table.*"),
                  attributes = cols
                )
              }
            DomainInfo(schemaName, tables = schemas)
          }
        case _ => throw new Exception("Failed to extract schemas and tables")
      }
    domains.toList
  }
}
