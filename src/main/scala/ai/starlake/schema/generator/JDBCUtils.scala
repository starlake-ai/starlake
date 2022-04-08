package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.model.{Attribute, Domain, Schema}
import com.typesafe.scalalogging.LazyLogging

import java.sql.DriverManager
import java.sql.Types._
import java.util.Properties
import java.util.regex.Pattern
import java.sql.{Connection => SQLConnection}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object JDBCUtils extends LazyLogging {

  type TableRemarks = String
  type Columns = List[Attribute]
  type PrimaryKeys = List[String]

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

  private def withJDBCConnection[T](
    connString: String
  )(f: SQLConnection => T)(implicit settings: Settings): T = {
    val jdbcOptions = settings.comet.connections(connString)

    // Only JDBC connections are supported
    assert(jdbcOptions.format == "jdbc")

    val url = jdbcOptions.options("url")
    val properties = new Properties()
    (jdbcOptions.options - "url").foreach { case (key, value) =>
      properties.setProperty(key, value)
    }
    val connection = DriverManager.getConnection(url, properties)
    try {
      val result = f(connection)
      result
    } finally
      Try(connection.close()) match {
        case Success(_) => logger.debug(s"Closed connection to $connString")
        case Failure(exception) =>
          logger.warn(s"Could not close connection to $connString", exception)
      }
  }

  def applyScript(script: String, connectionString: String)(implicit
    settings: Settings
  ): Boolean = {
    withJDBCConnection(connectionString) { conn =>
      conn.createStatement().execute(script)
    }
  }

  def extractJDBCTables(
    jdbcSchema: JDBCSchema
  )(implicit settings: Settings): Map[String, (TableRemarks, Columns, PrimaryKeys)] =
    withJDBCConnection(jdbcSchema.connection) { connection =>
      val databaseMetaData = connection.getMetaData()
      val jdbcTableMap =
        jdbcSchema.tables
          .map(tblSchema => tblSchema.name.toUpperCase -> tblSchema)
          .toMap
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
      logger.whenDebugEnabled {
        extractTables.keys.foreach(table => logger.debug(s"Found: $table"))
      } // If the user specified a list of table to extract we limit the table sot extract to those ones
      val selectedTables = tableNames match {
        case Nil =>
          allExtractedTables
        case list =>
          allExtractedTables.filter { case (table, _) =>
            list.contains(table.toUpperCase) || list.contains("*")
          }
      }
      logger.whenInfoEnabled {
        selectedTables.keys.foreach(table => logger.info(s"Selected: $table"))
      }

      // Extract the Comet Schema
      val selectedTablesAndColumns: Map[String, (TableRemarks, Columns, PrimaryKeys)] =
        selectedTables.map { case (tableName, tableRemarks) =>
          // Find all foreign keys
          val foreignKeysResultSet = databaseMetaData.getImportedKeys(
            jdbcSchema.catalog.orNull,
            jdbcSchema.schema,
            tableName
          )
          val foreignKeys = new Iterator[(String, String)] {
            def hasNext: Boolean = foreignKeysResultSet.next()

            def next(): (String, String) = {
              val pkSchemaName = foreignKeysResultSet.getString("PKTABLE_SCHEM")
              val pkTableName = foreignKeysResultSet.getString("PKTABLE_NAME")
              val pkColumnName = foreignKeysResultSet.getString("PKCOLUMN_NAME")
              val fkColumnName = foreignKeysResultSet.getString("FKCOLUMN_NAME").toUpperCase

              val pkCompositeName =
                if (pkSchemaName == null) s"$pkTableName.$pkColumnName"
                else s"$pkSchemaName.$pkTableName.$pkColumnName"

              fkColumnName -> pkCompositeName
            }
          }.toMap

          // Extract all columns
          val columnsResultSet = databaseMetaData.getColumns(
            jdbcSchema.catalog.orNull,
            jdbcSchema.schema,
            tableName,
            null
          )
          val attrs = new Iterator[Attribute] {
            def hasNext: Boolean = columnsResultSet.next()

            def next(): Attribute = {
              val colName = columnsResultSet.getString("COLUMN_NAME")
              logger.info(s"COLUMN_NAME=$tableName.$colName")
              val colType = columnsResultSet.getInt("DATA_TYPE")
              val colTypename = columnsResultSet.getString("TYPE_NAME")
              val colRemarks = columnsResultSet.getString("REMARKS")
              val colRequired = columnsResultSet.getString("IS_NULLABLE").equals("NO")
              val foreignKey = foreignKeys.get(colName.toUpperCase)

              Attribute(
                name = colName,
                `type` = sparkType(colType, tableName, colName, colTypename),
                required = colRequired,
                comment = Option(colRemarks),
                foreignKey = foreignKey
              )
            }
          }.to[ListBuffer]
          // remove duplicates
          // see https://stackoverflow.com/questions/1601203/jdbc-databasemetadata-getcolumns-returns-duplicate-columns
          val columns = attrs.groupBy(_.name).map { case (_, uniqAttr) => uniqAttr.head }

          logger.whenDebugEnabled {
            columns.foreach(column => logger.debug(s"column: $tableName.${column.name}"))
          }

          // Limit to the columns specified by the user if any
          val currentTableRequestedColumns =
            jdbcTableMap
              .get(tableName)
              .map(_.columns.getOrElse(Nil).map(_.toUpperCase))
              .getOrElse(Nil)
          val selectedColumns =
            if (currentTableRequestedColumns.isEmpty)
              columns.toList
            else
              columns.toList.filter(col =>
                currentTableRequestedColumns.contains(col.name.toUpperCase())
              )
          logger.whenDebugEnabled {
            columns.foreach(column =>
              logger.debug(s"Final schema column: $tableName.${column.name}")
            )
          }

          //      // Find primary keys
          val primaryKeysResultSet = databaseMetaData.getPrimaryKeys(
            jdbcSchema.catalog.orNull,
            jdbcSchema.schema,
            tableName
          )
          val primaryKeys = new Iterator[String] {
            def hasNext: Boolean = primaryKeysResultSet.next()

            def next(): String = primaryKeysResultSet.getString("COLUMN_NAME")
          }.toList
          tableName -> (tableRemarks, selectedColumns, primaryKeys)
        }
      selectedTablesAndColumns
    }

  def extractDomain(
    jdbcSchema: JDBCSchema,
    domainTemplate: Option[Domain],
    selectedTablesAndColumns: Map[String, (TableRemarks, Columns, PrimaryKeys)]
  ) = {
    val schemaMetadata =
      domainTemplate.flatMap(_.tables.headOption.flatMap(_.metadata))
    val cometSchema = selectedTablesAndColumns.map {
      case (tableName, (tableRemarks, selectedColumns, primaryKeys)) =>
        Schema(
          name = tableName,
          pattern = Pattern.compile(s"$tableName.*"),
          attributes = selectedColumns,
          metadata = schemaMetadata,
          merge = None,
          comment = Option(tableRemarks),
          presql = None,
          postsql = None,
          primaryKey = if (primaryKeys.isEmpty) None else Some(primaryKeys)
        )
    }
    // Generate the domain with a dummy watch directory
    val incomingDir = domainTemplate
      .map { dom =>
        DatasetArea
          .substituteDomainAndSchemaInPath(jdbcSchema.schema, "", dom.resolveDirectory())
          .toString
      }
      .getOrElse(s"/${jdbcSchema.schema}")

    val extensions = domainTemplate.flatMap(_.resolveExtensions())
    val ack = domainTemplate.flatMap(_.resolveAck())

    Domain(
      name = jdbcSchema.schema,
      metadata = domainTemplate
        .flatMap(_.metadata)
        .map(_.copy(directory = Some(incomingDir), extensions = extensions, ack = ack)),
      tableRefs = None,
      tables = cometSchema.toList,
      comment = None,
      extensions = None,
      ack = None
    )
  }

  private def sparkType(
    jdbcType: Int,
    tableName: String,
    colName: String,
    colTypename: String
  ): String = {
    val sqlType = reverseSqlTypes.getOrElse(jdbcType, colTypename)
    jdbcType match {
      case VARCHAR | CHAR | LONGVARCHAR          => "string"
      case BIT | BOOLEAN                         => "boolean"
      case DOUBLE | FLOAT | REAL                 => "double"
      case NUMERIC | DECIMAL                     => "decimal"
      case TINYINT | SMALLINT | INTEGER | BIGINT => "long"
      case DATE                                  => "date"
      case TIMESTAMP                             => "timestamp"
      case TIMESTAMP_WITH_TIMEZONE =>
        logger.warn(s"forced conversion for $tableName.$colName from $sqlType to timestamp")
        "timestamp"
      case VARBINARY | BINARY =>
        logger.warn(s"forced conversion for $tableName.$colName from $sqlType to string")
        "string"
      case _ =>
        logger.error(
          s"""Make sure user defined type  $colTypename is defined. Context: $tableName.$colName  -> $sqlType ($jdbcType)"""
        )
        colTypename
    }
  }

  def main(args: Array[String]): Unit = {
    JDBC2Yml.run(args)
  }
}
