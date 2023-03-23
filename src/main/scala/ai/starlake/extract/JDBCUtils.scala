package ai.starlake.extract

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.model.{Attribute, Domain, Schema}
import better.files.File
import com.typesafe.scalalogging.LazyLogging

import java.sql.Types._
import java.sql.{Connection => SQLConnection, DriverManager}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.Properties
import java.util.regex.Pattern
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try, Using}

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
    connectionOptions: Map[String, String]
  )(f: SQLConnection => T)(implicit settings: Settings): T = {
    assert(
      connectionOptions.contains("driver"),
      "driver class not found in JDBC connection options"
    )
    Class.forName(connectionOptions("driver"))
    val url = connectionOptions("url")
    val properties = new Properties()
    (connectionOptions - "url").foreach { case (key, value) =>
      properties.setProperty(key, value)
    }
    val connection = DriverManager.getConnection(url, properties)
    try {
      f(connection)
    } finally {
      Try(connection.close()) match {
        case Success(_) => logger.debug(s"Closed connection $url")
        case Failure(exception) =>
          logger.warn(s"Could not close connection to $url", exception)
      }
    }
  }

  def applyScript(script: String, connectionOptions: Map[String, String])(implicit
    settings: Settings
  ): Boolean = {
    withJDBCConnection(connectionOptions) { conn =>
      conn.createStatement().execute(script)
    }
  }

  private def getConnectionOptions(jdbcSchema: JDBCSchema) = {}

  def extractTableRemarks(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    table: String
  )(implicit
    settings: Settings
  ): Option[String] = {
    jdbcSchema.tableRemarks.map { remarks =>
      val sql = formatRemarksSQL(jdbcSchema, table, remarks)
      logger.info(s"Extracting table remarks using $sql")
      withJDBCConnection(connectionOptions) { connection =>
        val statement = connection.createStatement()
        val rs = statement.executeQuery(sql)
        if (rs.next()) {
          rs.getString(1)
        } else {
          logger.warn(s"Not table remark found for table $table")
          ""
        }
      }
    }
  }
  def extractColumnRemarks(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    table: String
  )(implicit
    settings: Settings
  ): Option[Map[TableRemarks, TableRemarks]] = {
    jdbcSchema.columnRemarks.map { remarks =>
      val sql = formatRemarksSQL(jdbcSchema, table, remarks)
      logger.info(s"Extracting column remarks using $sql")
      withJDBCConnection(connectionOptions) { connection =>
        val statement = connection.createStatement()
        val rs = statement.executeQuery(sql)
        val res = mutable.Map.empty[String, String]
        while (rs.next()) {
          res.put(rs.getString(1), rs.getString(2))
        }
        res.toMap
      }
    }
  }

  def extractJDBCTables(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String]
  )(implicit settings: Settings): Map[String, (TableRemarks, Columns, PrimaryKeys)] =
    withJDBCConnection(connectionOptions) { connection =>
      val databaseMetaData = connection.getMetaData()
      val jdbcTableMap =
        jdbcSchema.tables
          .map(tblSchema => tblSchema.name.toUpperCase -> tblSchema)
          .toMap
      val tableNames = jdbcTableMap.keys.toList

      /* Extract all tables from the database and return Map of tablename -> tableDescription */
      def extractTables(tablesToExtract: List[String] = Nil): Map[String, String] = {
        val tableNames = mutable.Map.empty[String, String]
        Using(
          databaseMetaData.getTables(
            jdbcSchema.catalog.orNull,
            jdbcSchema.schema,
            "%",
            jdbcSchema.tableTypes.toArray
          )
        ) { resultSet =>
          while (resultSet.next()) {
            val tableName = resultSet.getString("TABLE_NAME");
            if (tablesToExtract.isEmpty || tablesToExtract.contains(tableName.toUpperCase())) {
              val _remarks = extractTableRemarks(jdbcSchema, connectionOptions, tableName)
              val remarks = _remarks.getOrElse(resultSet.getString("REMARKS"))
              logger.info(s"Extracting table $tableName: $remarks")
              tableNames += tableName -> remarks
            }
          }
        }
        tableNames.toMap
      }
      val selectedTables = tableNames match {
        case Nil =>
          extractTables()
        case list if list.contains("*") =>
          extractTables()
        case list =>
          val extractedTableNames = extractTables(list)
          val notExtractedTable = list.diff(
            extractedTableNames.map { case (tableName, _) => tableName }.map(_.toUpperCase()).toList
          )
          if (notExtractedTable.nonEmpty) {
            val tablesNotExtractedStr = notExtractedTable.mkString(", ")
            logger.warn(
              s"The following tables where not extracted for ${jdbcSchema.schema} : $tablesNotExtractedStr"
            )
          }
          extractedTableNames
      }
      logger.whenInfoEnabled {
        selectedTables.keys.foreach(table => logger.info(s"Selected: $table"))
      }
      // Extract the Comet Schema
      val selectedTablesAndColumns: Map[String, (TableRemarks, Columns, PrimaryKeys)] =
        selectedTables.map { case (tableName, tableRemarks) =>
          Using.Manager { use =>
            // Find all foreign keys

            val foreignKeysResultSet = use(
              databaseMetaData.getImportedKeys(
                jdbcSchema.catalog.orNull,
                jdbcSchema.schema,
                tableName
              )
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
            val columnsResultSet = use(
              databaseMetaData.getColumns(
                jdbcSchema.catalog.orNull,
                jdbcSchema.schema,
                tableName,
                null
              )
            )
            val remarks =
              extractColumnRemarks(jdbcSchema, connectionOptions, tableName).getOrElse(Map.empty)

            val isPostgres = connectionOptions("url").contains("postgres")
            val attrs = new Iterator[Attribute] {
              def hasNext: Boolean = columnsResultSet.next()

              def next(): Attribute = {
                val colName = columnsResultSet.getString("COLUMN_NAME")
                logger.info(s"COLUMN_NAME=$tableName.$colName")
                val colType = columnsResultSet.getInt("DATA_TYPE")
                val colTypename = columnsResultSet.getString("TYPE_NAME")
                val colRemarks =
                  remarks.getOrElse(colName, columnsResultSet.getString("REMARKS"))
                val colRequired = columnsResultSet.getString("IS_NULLABLE").equals("NO")
                val foreignKey = foreignKeys.get(colName.toUpperCase)
                val colDecimalDigits = Option(columnsResultSet.getInt("DECIMAL_DIGITS"))
                // val columnPosition = columnsResultSet.getInt("ORDINAL_POSITION")
                Attribute(
                  name = colName,
                  `type` = sparkType(
                    colType,
                    tableName,
                    colName,
                    colTypename,
                    colDecimalDigits,
                    isPostgres
                  ),
                  required = colRequired,
                  comment = Option(colRemarks),
                  foreignKey = foreignKey
                )
              }
            }.to[ListBuffer]

            // remove duplicates
            // see https://stackoverflow.com/questions/1601203/jdbc-databasemetadata-getcolumns-returns-duplicate-columns
            def removeDuplicatesColumns(list: List[Attribute]): List[Attribute] =
              list.foldLeft(List.empty[Attribute]) { (partialResult, element) =>
                if (partialResult.exists(_.name == element.name)) partialResult
                else partialResult :+ element
              }

            val columns = removeDuplicatesColumns(attrs.toList)

            logger.whenDebugEnabled {
              columns.foreach(column => logger.debug(s"column: $tableName.${column.name}"))
            }

            // Limit to the columns specified by the user if any
            val currentTableRequestedColumns =
              jdbcTableMap
                .get(tableName)
                .map(_.columns.map(_.toUpperCase))
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
            val primaryKeysResultSet = use(
              databaseMetaData.getPrimaryKeys(
                jdbcSchema.catalog.orNull,
                jdbcSchema.schema,
                tableName
              )
            )
            val primaryKeys = new Iterator[String] {
              def hasNext: Boolean = primaryKeysResultSet.next()

              def next(): String = primaryKeysResultSet.getString("COLUMN_NAME")
            }.toList
            tableName -> (tableRemarks, selectedColumns, primaryKeys)
          } match {
            case Failure(exception) => throw exception
            case Success(value)     => value
          }
        }
      selectedTablesAndColumns
    }

  private def formatRemarksSQL(
    jdbcSchema: JDBCSchema,
    table: String,
    remarks: String
  )(implicit settings: Settings): String = {
    import ai.starlake.utils.Formatter._
    val parameters = Map(
      "catalog" -> jdbcSchema.catalog.getOrElse(""),
      "schema"  -> jdbcSchema.schema,
      "table"   -> table
    )
    logger.info(s"Interpolating remark $remarks with parameters $parameters")
    val sql = remarks.richFormat(
      parameters,
      Map.empty
    )
    logger.info(s"Remark interpolated as $sql")
    sql
  }

  def extractDomain(
    jdbcSchema: JDBCSchema,
    domainTemplate: Option[Domain],
    selectedTablesAndColumns: Map[String, (TableRemarks, Columns, PrimaryKeys)]
  ) = {
    val trimTemplate =
      domainTemplate.flatMap(_.tables.headOption.flatMap(_.attributes.head.trim))

    val cometSchema = selectedTablesAndColumns.map {
      case (tableName, (tableRemarks, selectedColumns, primaryKeys)) =>
        Schema(
          name = tableName,
          pattern = Pattern.compile(s"$tableName.*"),
          attributes = selectedColumns.map(_.copy(trim = trimTemplate)),
          metadata = None,
          merge = None,
          comment = Option(tableRemarks),
          presql = Nil,
          postsql = Nil,
          primaryKey = primaryKeys
        )
    }
    val domainName = jdbcSchema.schema.replaceAll("[^\\p{Alnum}]", "_")
    // Generate the domain with a dummy watch directory
    val incomingDir = domainTemplate
      .map { dom =>
        DatasetArea
          .substituteDomainAndSchemaInPath(domainName, "", dom.resolveDirectory())
          .toString
      }
      .getOrElse(s"/${jdbcSchema.schema}")

    val extensions = domainTemplate.map(_.resolveExtensions()).getOrElse(Nil)
    val ack = domainTemplate.flatMap(_.resolveAck())

    Domain(
      name = domainName,
      metadata = domainTemplate
        .flatMap(_.metadata)
        .map(_.copy(directory = Some(incomingDir), extensions = extensions, ack = ack)),
      tableRefs = Nil,
      tables = cometSchema.toList,
      comment = None,
      extensions = Nil,
      ack = None
    )
  }

  private def sparkType(
    jdbcType: Int,
    tableName: String,
    colName: String,
    colTypename: String,
    decimalDigit: Option[Int],
    /*
    because getInt(DECIMAL_DIGITS) returns 0 when this value is null and because
    FROM Postgres DOC
    without any precision or scale creates an “unconstrained numeric” column in which numeric values of any length
    can be stored, up to the implementation limits. A column of this kind will not coerce input values to any particular scale,
    whereas numeric columns with a declared scale will coerce input values to that scale.
    (The SQL standard requires a default scale of 0, i.e., coercion to integer precision. We find this a bit useless.
    If you're concerned about portability, always specify the precision and scale explicitly.)
     */
    isPostgres: Boolean
  ): String = {
    val sqlType = reverseSqlTypes.getOrElse(jdbcType, colTypename)
    jdbcType match {
      case VARCHAR | CHAR | LONGVARCHAR => "string"
      case BIT | BOOLEAN                => "boolean"
      case DOUBLE | FLOAT | REAL        => "double"
      case NUMERIC =>
        decimalDigit match {
          case Some(0) if isPostgres => "decimal"
          case Some(0)               => "long"
          case _                     => "decimal"
        }
      case DECIMAL =>
        decimalDigit match {
          case Some(0) => "long"
          case _       => "decimal"
        }
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

  def extractData(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    baseOutputDir: File,
    limit: Int,
    separator: String
  )(implicit
    settings: Settings
  ): Unit = {
    val domainName = jdbcSchema.schema.replaceAll("[^\\p{Alnum}]", "_")
    baseOutputDir.createDirectories()
    val outputDir = File(baseOutputDir, domainName)
    outputDir.createDirectories()
    val selectedTablesAndColumns = JDBCUtils.extractJDBCTables(jdbcSchema, connectionOptions)
    selectedTablesAndColumns.foreach {
      case (tableName, (tableRemarks, selectedColumns, primaryKeys)) =>
        val formatter = DateTimeFormatter
          .ofPattern("yyyyMMddHHmmss")
          .withZone(ZoneOffset.UTC)
        val dateTime = formatter.format(Instant.now())
        val outFile = File(outputDir, tableName + s"-$dateTime.csv")
        val cols = selectedColumns.map(_.name).mkString(",")
        val headers = selectedColumns.map(_.name).mkString(";")
        logger.info(s"Exporting data to file ${outFile.pathAsString}")
        outFile.parent.createDirectories()
        outFile.delete(true)
        val outFileWriter = outFile.newFileWriter(append = false)
        Try {
          outFileWriter.append(headers + "\n")
          val sql = s"select $cols from ${jdbcSchema.schema}.$tableName"
          withJDBCConnection(connectionOptions) { connection =>
            val statement = connection.createStatement()
            // 0 means no limit
            statement.setMaxRows(limit)
            val rs = statement.executeQuery(sql)
            val rsMetadata = rs.getMetaData()
            while (rs.next()) {
              val colList = ListBuffer.empty[String]
              for (icol <- 1 to rsMetadata.getColumnCount) {
                val obj = Option(rs.getObject(icol))
                val sqlType = rsMetadata.getColumnType(icol)
                import java.sql.Types
                val colValue = sqlType match {
                  case Types.VARCHAR =>
                    rs.getString(icol)
                  case Types.NULL =>
                    "null"
                  case Types.CHAR =>
                    "\"" + obj.map(_ => rs.getString(icol)).getOrElse("") + "\""
                  case Types.TIMESTAMP =>
                    "\"" + obj.map(_ => rs.getTimestamp(icol).toString).getOrElse("") + "\""
                  case Types.DOUBLE =>
                    obj.map(_ => rs.getDouble(icol).toString).getOrElse("")
                  case Types.INTEGER =>
                    obj.map(_ => rs.getInt(icol).toString).getOrElse("")
                  case Types.SMALLINT =>
                    obj.map(_ => rs.getInt(icol).toString).getOrElse("")
                  case Types.DECIMAL =>
                    obj.map(_ => rs.getBigDecimal(icol).toString).getOrElse("")
                  case _ =>
                    "\"" + obj.map(_.toString).getOrElse("") + "\""
                }
                colList.append(colValue)
              }
              val rowString = colList.mkString(separator)
              outFileWriter.append(rowString + "\n")
            }
          }
        } match {
          case Success(_) =>
            outFileWriter.close()
          case Failure(_) =>
            outFileWriter.close()
            outFile.delete(true)
        }
    }
  }
}
