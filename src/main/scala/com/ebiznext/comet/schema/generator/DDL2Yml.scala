package com.ebiznext.comet.schema.generator

import better.files.File
import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.schema.model.{Attribute, Domain, Schema}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import java.sql.DriverManager
import java.sql.Types._
import java.util.Properties
import java.util.regex.Pattern
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

object DDL2Yml extends LazyLogging {

  // java.sql.Types
  val sqlTypes = Map(
    "BIT"                     -> -7,
    "TINYINT"                 -> -6,
    "SMALLINT"                -> 5,
    "INTEGER"                 -> 4,
    "BIGINT"                  -> -5,
    "FLOAT"                   -> 6,
    "REAL"                    -> 7,
    "DOUBLE"                  -> 8,
    "NUMERIC"                 -> 2,
    "DECIMAL"                 -> 3,
    "CHAR"                    -> 1,
    "VARCHAR"                 -> 12,
    "LONGVARCHAR"             -> -1,
    "DATE"                    -> 91,
    "TIME"                    -> 92,
    "TIMESTAMP"               -> 93,
    "BINARY"                  -> -2,
    "VARBINARY"               -> -3,
    "LONGVARBINARY"           -> -4,
    "NULL"                    -> 0,
    "OTHER"                   -> 1111,
    "BOOLEAN"                 -> 16,
    "NVARCHAR"                -> -9,
    "NCHAR"                   -> -15,
    "LONGNVARCHAR"            -> -16,
    "TIME_WITH_TIMEZONE"      -> 2013,
    "TIMESTAMP_WITH_TIMEZONE" -> 2014
  )

  // The other part of the biMap
  val reverseSqlTypes = sqlTypes map (_.swap)

  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    DDL2YmlConfig.parse(args) match {
      case Some(config) =>
        run(config)
      case None => throw new Exception(s"Could not parse arguments ${args.mkString("")}")
    }
  }

  /** Generate YML file from JDBC Schema stoerd in a YML file
    *
    * @param jdbcMapFile  : Yaml File containing the JDBC Schema to extract
    * @param ymlOutputDir : Where to output the YML file. The generated filename
    *                     will be in the for TABLE_SCHEMA_NAME.yml
    * @param settings     : Application configuration file
    */
  def run(config: DDL2YmlConfig)(implicit settings: Settings): Unit = {
    val jdbcSchema = YamlSerializer.deserializeJDBCSchema(File(config.jdbcMapping))
    val domainTemplate = config.ymlTemplate.map { ymlTemplate =>
      YamlSerializer.deserializeDomain(ymlTemplate) match {
        case Success(domain) => domain
        case Failure(e)      => throw e
      }
    }
    run(jdbcSchema, File(config.outputDir), domainTemplate)
  }

  /** Generate YML file from the JDBCSchema
    *
    * @param jdbcSchema   : the JDBC Schema to extract
    * @param ymlOutputDir : Where to output the YML file. The generated filename
    *                     will be in the for TABLE_SCHEMA_NAME.yml
    * @param settings     : Application configuration file
    */
  def run(jdbcSchema: JDBCSchema, ymlOutputDir: File, domainTemplate: Option[Domain])(implicit
    settings: Settings
  ): Unit = {
    val jdbcOptions = settings.comet.connections(jdbcSchema.connection)
    // Only JDBC connections are supported
    assert(jdbcOptions.format == "jdbc")
    val url = jdbcOptions.options("url")
    val properties = new Properties()
    (jdbcOptions.options - "url").foreach { case (key, value) =>
      properties.setProperty(key, value)
    }
    val connection = DriverManager.getConnection(url, properties)
    val databaseMetaData = connection.getMetaData()
    val jdbcTableMap =
      jdbcSchema.tables.map(tblSchema => tblSchema.name.toUpperCase -> tblSchema).toMap
    val tableNames = jdbcTableMap.keys.toList

    /* Extract all tables from the database and return Map of tablename -> tableDescription */
    def extractTables(): Map[String, String] = {
      val tableNames = mutable.Map.empty[String, String]
      val resultSet = databaseMetaData.getTables(
        jdbcSchema.catalog.orNull,
        jdbcSchema.schema,
        "%",
        jdbcSchema.tableTypes.toArray
      )
      while (resultSet.next()) {
        val tableName = resultSet.getString("TABLE_NAME");
        val remarks = resultSet.getString("REMARKS");
        tableNames += tableName -> remarks
      }
      resultSet.close()
      tableNames.toMap
    }

    val allExtractedTables = extractTables()
    // If the user specified a list of table to extract we limit the table sot extract to those ones
    val selectedTables = tableNames match {
      case Nil =>
        allExtractedTables
      case list =>
        allExtractedTables.filter { case (table, _) => list.contains(table.toUpperCase) }
    }

    val schemaMetadata = domainTemplate.flatMap(_.schemas.headOption.flatMap(_.metadata))
    // Extract the Comet Schema
    val cometSchema = selectedTables.map { case (tableName, tableRemarks) =>
      val resultSet = databaseMetaData.getColumns(
        jdbcSchema.catalog.orNull,
        jdbcSchema.schema,
        tableName,
        null
      )
      val columns = ListBuffer.empty[Attribute]
      while (resultSet.next()) {
        val colName = resultSet.getString("COLUMN_NAME")
        val colType = resultSet.getInt("DATA_TYPE")
        val colRemarks = resultSet.getString("REMARKS")
        val colRequired = resultSet.getString("IS_NULLABLE").equals("NO")
        columns += Attribute(
          name = colName,
          `type` = sparkType(colType),
          required = colRequired,
          comment = Option(colRemarks)
        )
      }
      // Limit to the columns specified by the user if any
      val currentTableRequestedColumns =
        jdbcTableMap.get(tableName).map(_.columns.map(_.toUpperCase)).getOrElse(Nil)
      val selectedColumns =
        if (currentTableRequestedColumns.isEmpty)
          columns.toList
        else
          columns.toList.filter(col =>
            currentTableRequestedColumns.contains(col.name.toUpperCase())
          )
      Schema(
        tableName,
        Pattern.compile(s"$tableName.*"),
        selectedColumns,
        schemaMetadata,
        None,
        Option(tableRemarks),
        None,
        None
      )
    }
    // Generate the domain with a dummy watch directory
    val incomingDir = domainTemplate
      .map { dom =>
        DatasetArea.substituteDomainAndSchemaInPath(jdbcSchema.schema, "", dom.directory).toString
      }
      .getOrElse(s"/${jdbcSchema.schema}")

    val domain =
      Domain(jdbcSchema.schema, incomingDir, domainTemplate.flatMap(_.metadata), cometSchema.toList)
    YamlSerializer.serializeToFile(File(ymlOutputDir, jdbcSchema.schema + ".yml"), domain)
    connection.close()
  }

  private def sparkType(jdbcType: Int): String = {
    jdbcType match {
      case VARCHAR | CHAR | LONGVARCHAR          => "string"
      case BIT | BOOLEAN                         => "boolean"
      case FLOAT | REAL | DOUBLE                 => "double"
      case NUMERIC                               => "decimal"
      case TINYINT | SMALLINT | INTEGER | BIGINT => "long"
      case DATE                                  => "date"
      case TIMESTAMP | TIMESTAMP_WITH_TIMEZONE   => "timestamp"
      case _ =>
        logger.error(
          s"""unsupported COLUMN TYPE -> ${reverseSqlTypes
            .getOrElse(jdbcType, "UNKNOWN JDBC TYPE")}($jdbcType)"""
        )
        reverseSqlTypes.getOrElse(jdbcType, s"UNKNOWN JDBC TYPE => $jdbcType")
    }
  }

  def main(args: Array[String]): Unit = {
    val result = DDL2Yml.run(args)
  }
}
