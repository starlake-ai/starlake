package ai.starlake.extract

import ai.starlake.config.Settings.{Connection, JdbcEngine}
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.JdbcDbUtils.{lastExportTableName, Columns}
import ai.starlake.schema.model._
import ai.starlake.utils.{SparkUtils, Utils}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types._

import java.sql.Types._
import java.sql.{
  Connection => SQLConnection,
  DatabaseMetaData,
  Date,
  DriverManager,
  PreparedStatement,
  ResultSet,
  Timestamp
}
import java.util.Properties
import java.util.regex.Pattern
import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.{Failure, Success, Try, Using}

object JdbcDbUtils extends LazyLogging {

  type TableName = String
  type TableRemarks = String
  type ColumnName = String
  type Columns = List[Attribute]
  type PrimaryKeys = List[String]

  // java.sql.Types
  private val sqlTypes = Map(
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

  val lastExportTableName = "SL_LAST_EXPORT"

  // The other part of the biMap
  private val reverseSqlTypes = sqlTypes map (_.swap)

  /** Execute a block of code in the context of a newly created connection. We better use here a
    * Connection pool, but since starlake processes are short lived, we do not really need it.
    *
    * @param connectionSettings
    * @param f
    * @param settings
    * @tparam T
    * @return
    */
  def withJDBCConnection[T](
    connectionOptions: Map[String, String]
  )(f: SQLConnection => T)(implicit settings: Settings): T = {
    assert(
      connectionOptions.contains("driver"),
      s"driver class not found in JDBC connection options $connectionOptions"
    )
    Class.forName(connectionOptions("driver"))
    val url = connectionOptions("url")
    val properties = new Properties()
    (connectionOptions - "url" - "driver").foreach { case (key, value) =>
      properties.setProperty(key, value)
    }
    val connection = DriverManager.getConnection(url, properties)
    // connection.setAutoCommit(false)

    val result = Try {
      f(connection)
    } match {
      case Failure(exception) =>
        logger.error(s"Error running sql", exception)
        // connection.rollback()
        Failure(throw exception)
      case Success(value) =>
        // connection.commit()
        Success(value)
    }

    Try(connection.close()) match {
      case Success(_) => logger.debug(s"Closed connection $url")
      case Failure(exception) =>
        logger.warn(s"Could not close connection to $url", exception)
    }
    result match {
      case Failure(exception) =>
        throw exception
      case Success(value) => value
    }
  }

  @throws[Exception]
  def createSchema(conn: SQLConnection, domainName: String): Unit = {
    executeUpdate(s"CREATE SCHEMA IF NOT EXISTS $domainName", conn) match {
      case Success(_) =>
      case Failure(e) =>
        logger.error(s"Error creating schema $domainName", e)
        throw e
    }
  }

  @throws[Exception]
  def dropTable(tableName: String, conn: SQLConnection): Unit = {
    executeUpdate(s"DROP TABLE IF EXISTS $tableName", conn) match {
      case Success(_) =>
      case Failure(e) =>
        logger.error(s"Error creating schema $tableName", e)
        throw e
    }
  }

  def tableExists(conn: SQLConnection, url: String, domainAndTablename: String): Boolean = {
    val dialect = SparkUtils.dialect(url)
    Try {
      val statement = conn.prepareStatement(dialect.getTableExistsQuery(domainAndTablename))
      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    } match {
      case Failure(e) =>
        logger.info(s"Table $domainAndTablename does not exist")
        false
      case Success(_) => true
    }
  }

  @throws[Exception]
  def executeAlterTable(script: String, connection: java.sql.Connection): Boolean = {
    val metadata = connection.getMetaData()
    val isAutoCommit = connection.getAutoCommit()
    if (!metadata.supportsTransactions) {
      throw new Exception("Database does not support alter table feature")
    } else {
      connection.setAutoCommit(false)
    }
    val statement = connection.createStatement()
    try {
      val res = statement.executeUpdate(script)
      connection.commit()
      true
    } finally {
      statement.close()
      connection.setAutoCommit(isAutoCommit)
    }
  }

  def execute(script: String, connection: java.sql.Connection): Try[Boolean] = {
    val statement = connection.createStatement()
    val result = Try {
      statement.execute(script)
    }
    result match {
      case Failure(exception) =>
        logger.error(s"Error running sql $script", exception)
        throw exception
      case Success(value) => value
    }
    statement.close()
    result
  }

  def executeUpdate(script: String, connection: java.sql.Connection): Try[Boolean] = {
    logger.info(s"Running $script")
    val statement = connection.createStatement()
    val result = Try {
      val count = statement.executeUpdate(script)
      logger.info(s"$count records affected")
      true
    }
    result match {
      case Failure(exception) =>
        logger.error(s"Error running $script", exception)
        throw exception
      case Success(value) =>
        logger.info(s"Executed $script with return value $value")
    }
    statement.close()
    result
  }

  /** RUn the sql statement in the context of a connection
    *
    * @param script
    * @param connectionOptions
    * @param settings
    * @return
    */
  def execute(script: String, connectionOptions: Map[String, String])(implicit
    settings: Settings
  ): Boolean = {
    withJDBCConnection(connectionOptions) { conn =>
      conn.createStatement().execute(script)
    }
  }

  /** Get table comments for a data base schema
    *
    * @param jdbcSchema
    * @param connectionOptions
    * @param table
    * @param settings
    * @return
    */
  private def extractTableRemarks(
    jdbcSchema: JDBCSchema,
    connection: SQLConnection,
    table: String,
    jdbcEngine: Option[JdbcEngine]
  )(implicit
    settings: Settings
  ): Option[String] = {
    val tableRemarks = jdbcSchema.tableRemarks.orElse(jdbcEngine.flatMap(_.tableRemarks))
    tableRemarks.map { remarks =>
      val sql = formatRemarksSQL(jdbcSchema, table, remarks)
      logger.debug(s"Extracting table remarks using $sql")
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

  /** Get column comments for a database table
    *
    * @param jdbcSchema
    * @param connectionOptions
    * @param table
    * @param settings
    * @return
    */
  private def extractColumnRemarks(
    jdbcSchema: JDBCSchema,
    connectionSettings: Connection,
    table: String,
    jdbcEngine: Option[JdbcEngine]
  )(implicit
    settings: Settings
  ): Option[Map[TableRemarks, TableRemarks]] = {
    val columnRemarks = jdbcSchema.columnRemarks.orElse(jdbcEngine.flatMap(_.columnRemarks))
    columnRemarks.map { remarks =>
      val sql = formatRemarksSQL(jdbcSchema, table, remarks)
      logger.debug(s"Extracting column remarks using $sql")
      withJDBCConnection(connectionSettings.options) { connection =>
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

  private def extractCaseInsensitiveSchemaName(
    connectionSettings: Connection,
    databaseMetaData: DatabaseMetaData,
    schemaName: String
  ): Try[String] = {
    connectionSettings match {
      case d if d.isMySQLOrMariaDb() =>
        Using(
          databaseMetaData.getCatalogs()
        ) { resultSet =>
          var result: Option[String] = None
          while (result.isEmpty && resultSet.next()) {
            val tableSchema = resultSet.getString("TABLE_CAT")
            if (schemaName.equalsIgnoreCase(tableSchema)) {
              result = Some(tableSchema)
            }
          }
          result.getOrElse(throw new Exception(s"Schema $schemaName not found"))
        }
      case _ =>
        Using(
          databaseMetaData.getSchemas()
        ) { resultSet =>
          var result: Option[String] = None
          while (result.isEmpty && resultSet.next()) {
            val tableSchema = resultSet.getString("TABLE_SCHEM")
            if (schemaName.equalsIgnoreCase(tableSchema)) {
              result = Some(tableSchema)
            }
          }
          result.getOrElse(throw new Exception(s"Schema $schemaName not found"))
        }
    }
  }

  /** Get all tables, columns and primary keys of a database schema as described in the YML file.
    * Exclude tables not selected in the YML file
    *
    * @param jdbcSchema
    * @param connectionOptions
    * @param settings
    * @return
    */
  def extractJDBCTables(
    jdbcSchema: JDBCSchema,
    connectionSettings: Connection,
    skipRemarks: Boolean,
    keepOriginalName: Boolean
  )(implicit
    settings: Settings,
    fjp: Option[ForkJoinTaskSupport]
  ): Map[TableName, ExtractTableAttributes] =
    withJDBCConnection(connectionSettings.options) { connection =>
      val url = connectionSettings.options("url")
      val jdbcServer = url.split(":")(1)
      val jdbcEngine = settings.appConfig.jdbcEngines.get(jdbcServer)

      val databaseMetaData = connection.getMetaData()

      val jdbcTableMap =
        jdbcSchema.tables
          .map(tblSchema => tblSchema.name.toUpperCase -> tblSchema)
          .toMap
      val uppercaseTableNames = jdbcTableMap.keys.toList

      /* Extract all tables from the database and return Map of tablename -> tableDescription */
      def extractTables(
        schemaName: String,
        tablePredicate: String => Boolean
      ): Map[String, String] = {
        val tableNames = mutable.Map.empty[String, String]
        Try {
          connectionSettings match {
            case d if d.isMySQLOrMariaDb() =>
              databaseMetaData.getTables(
                schemaName,
                "%",
                "%",
                jdbcSchema.tableTypes.toArray
              )
            case d if d.isDuckDb() =>
              // https://duckdb.org/docs/sql/information_schema.html
              val tableTypes = jdbcSchema.tableTypes.map { tt =>
                if (tt == "TABLE")
                  "BASE TABLE"
                else
                  tt

              }.toArray
              databaseMetaData.getTables(
                jdbcSchema.catalog.orNull,
                schemaName,
                "%",
                tableTypes
              )
            case _ =>
              databaseMetaData.getTables(
                jdbcSchema.catalog.orNull,
                schemaName,
                "%",
                jdbcSchema.tableTypes.toArray
              )
          }
        } match {
          case Success(resultSet) =>
            Using(resultSet) { resultSet =>
              while (resultSet.next()) {
                val tableName = resultSet.getString("TABLE_NAME")
                if (tablePredicate(tableName)) {
                  val localRemarks =
                    if (skipRemarks) None
                    else
                      Try {
                        extractTableRemarks(jdbcSchema, connection, tableName, jdbcEngine)
                      } match {
                        case Failure(exception) =>
                          logger.warn(exception.getMessage, exception)
                          None
                        case Success(value) => value
                      }
                  val remarks = localRemarks.getOrElse(resultSet.getString("REMARKS"))
                  tableNames += tableName -> remarks
                }
              }
            }
          case Failure(exception) =>
            logger.warn(Utils.exceptionAsString(exception))
            logger.warn(s"The following schema could not be found $schemaName")
        }
        tableNames.toMap
      }

      extractCaseInsensitiveSchemaName(
        connectionSettings,
        databaseMetaData,
        jdbcSchema.schema
      ).map { schemaName =>
        val lowerCasedExcludeTables = jdbcSchema.exclude.map(_.toLowerCase)

        def tablesInScopePredicate(tablesToExtract: List[String] = Nil) =
          (tableName: String) => {
            !lowerCasedExcludeTables.contains(
              tableName.toLowerCase
            ) && (tablesToExtract.isEmpty || tablesToExtract.contains(tableName.toUpperCase()))
          }

        val selectedTables = uppercaseTableNames match {
          case Nil =>
            extractTables(schemaName, tablesInScopePredicate())
          case list if list.contains("*") =>
            extractTables(schemaName, tablesInScopePredicate())
          case list =>
            val extractedTableNames =
              extractTables(schemaName, tablesInScopePredicate(list))
            val notExtractedTable = list.diff(
              extractedTableNames
                .map { case (tableName, _) => tableName }
                .map(_.toUpperCase())
                .toList
            )
            if (notExtractedTable.nonEmpty) {
              val tablesNotExtractedStr = notExtractedTable.mkString(", ")
              logger.warn(
                s"The following tables where not extracted for ${jdbcSchema.schema} : $tablesNotExtractedStr"
              )
            }
            extractedTableNames
        }
        logger.whenDebugEnabled {
          selectedTables.keys.foreach(table => logger.debug(s"Selected: $table"))
        }
        // Extract the Starlake Schema
        val selectedTablesAndColumnsAndFilter: Map[TableName, ExtractTableAttributes] =
          ParUtils
            .makeParallel(selectedTables.toList)
            .map { case (tableName, tableRemarks) =>
              val jdbcTableColumnsOpt =
                jdbcSchema.tables.find(_.name.equalsIgnoreCase(tableName)).map(_.columns)
              ExtractUtils.timeIt(s"Table extraction of $tableName") {
                logger.info(s"Extracting table $tableName: $tableRemarks")
                withJDBCConnection(connectionSettings.options) { tableExtractConnection =>
                  val databaseMetaData = tableExtractConnection.getMetaData
                  Using.Manager { use =>
                    // Find all foreign keys

                    val foreignKeys: Map[String, String] =
                      Try {
                        new Iterator[(String, String)] {
                          val foreignKeysResultSet = use(
                            connectionSettings match {
                              case d if d.isMySQLOrMariaDb() =>
                                databaseMetaData.getImportedKeys(
                                  schemaName,
                                  None.orNull,
                                  tableName
                                )
                              case _ =>
                                databaseMetaData.getImportedKeys(
                                  jdbcSchema.catalog.orNull,
                                  schemaName,
                                  tableName
                                )
                            }
                          )

                          def hasNext: Boolean = foreignKeysResultSet.next()

                          def next(): (String, String) = {
                            val pkSchemaName = foreignKeysResultSet.getString("PKTABLE_SCHEM")
                            val pkTableName = foreignKeysResultSet.getString("PKTABLE_NAME")
                            val pkColumnName = foreignKeysResultSet.getString("PKCOLUMN_NAME")
                            val pkFinalColumnName = if (keepOriginalName) {
                              pkColumnName
                            } else {
                              val pkTableColumnsOpt = jdbcSchema.tables
                                .find(_.name.equalsIgnoreCase(pkTableName))
                                .map(_.columns)
                              pkTableColumnsOpt
                                .flatMap(
                                  _.find(_.name.equalsIgnoreCase(pkColumnName)).flatMap(_.rename)
                                )
                                .getOrElse(pkColumnName)
                            }
                            val fkColumnName =
                              foreignKeysResultSet.getString("FKCOLUMN_NAME").toUpperCase

                            val pkCompositeName =
                              if (pkSchemaName == null) s"$pkTableName.$pkFinalColumnName"
                              else s"$pkSchemaName.$pkTableName.$pkFinalColumnName"

                            fkColumnName -> pkCompositeName
                          }
                        }.toMap
                      } match {
                        case Failure(exception) =>
                          logger.warn(
                            s"Could not extract foreign keys for table $tableName",
                            exception
                          )
                          Map.empty[String, String]
                        case Success(value) => value
                      }
                    // Extract all columns
                    val columnsResultSet = use(
                      connectionSettings match {
                        case d if d.isMySQLOrMariaDb() =>
                          databaseMetaData.getColumns(
                            schemaName,
                            None.orNull,
                            tableName,
                            None.orNull
                          )
                        case _ =>
                          databaseMetaData.getColumns(
                            jdbcSchema.catalog.orNull,
                            schemaName,
                            tableName,
                            None.orNull
                          )
                      }
                    )
                    val remarks: Map[TableRemarks, TableRemarks] = (
                      if (skipRemarks) None
                      else
                        extractColumnRemarks(jdbcSchema, connectionSettings, tableName, jdbcEngine)
                    ).getOrElse(Map.empty)

                    val attrs = new Iterator[Attribute] {
                      def hasNext: Boolean = columnsResultSet.next()

                      def next(): Attribute = {
                        val colName = columnsResultSet.getString("COLUMN_NAME")
                        val finalColName =
                          jdbcTableColumnsOpt
                            .flatMap(_.find(_.name.equalsIgnoreCase(colName)))
                            .flatMap(_.rename)
                            .getOrElse(colName)
                        logger.debug(
                          s"COLUMN_NAME=$tableName.$colName and its extracted name is $finalColName"
                        )
                        val colType = columnsResultSet.getInt("DATA_TYPE")
                        val colTypename = columnsResultSet.getString("TYPE_NAME")
                        val colRemarks =
                          remarks.getOrElse(colName, columnsResultSet.getString("REMARKS"))
                        val colRequired = columnsResultSet.getString("IS_NULLABLE").equals("NO")
                        val foreignKey = foreignKeys.get(colName.toUpperCase)
                        val colDecimalDigits = Option(columnsResultSet.getInt("DECIMAL_DIGITS"))
                        // val columnPosition = columnsResultSet.getInt("ORDINAL_POSITION")
                        Attribute(
                          name = if (keepOriginalName) colName else finalColName,
                          rename = if (keepOriginalName) Some(finalColName) else None,
                          `type` = sparkType(
                            colType,
                            tableName,
                            colName,
                            colTypename
                          ),
                          required = colRequired,
                          comment = Option(colRemarks),
                          foreignKey = foreignKey
                        )
                      }
                    }.toList

                    // remove duplicates
                    // see https://stackoverflow.com/questions/1601203/jdbc-databasemetadata-getcolumns-returns-duplicate-columns
                    def removeDuplicatesColumns(list: List[Attribute]): List[Attribute] =
                      list.foldLeft(List.empty[Attribute]) { (partialResult, element) =>
                        if (partialResult.exists(_.name == element.name)) partialResult
                        else partialResult :+ element
                      }

                    val columns = removeDuplicatesColumns(attrs)

                    logger.whenDebugEnabled {
                      columns.foreach(column => logger.debug(s"column: $tableName.${column.name}"))
                    }

                    val jdbcCurrentTable = jdbcTableMap
                      .get(tableName.toUpperCase)
                    // Limit to the columns specified by the user if any
                    val currentTableRequestedColumns: Map[ColumnName, Option[ColumnName]] =
                      jdbcCurrentTable
                        .map(_.columns.map(c => c.name.toUpperCase.trim -> c.rename))
                        .getOrElse(Map.empty)
                        .toMap
                    val currentFilter = jdbcCurrentTable.flatMap(_.filter)
                    val selectedColumns: List[Attribute] =
                      (if (
                         currentTableRequestedColumns.isEmpty || currentTableRequestedColumns
                           .contains("*")
                       )
                         columns
                       else
                         columns.filter(col =>
                           currentTableRequestedColumns.contains(col.name.toUpperCase())
                         )).map(c =>
                        c.copy(rename =
                          currentTableRequestedColumns.get(c.name.toUpperCase).flatten
                        )
                      )
                    logger.whenDebugEnabled {
                      val schemaMessage = selectedColumns
                        .map(c =>
                          c.name -> c.rename match {
                            case (name, Some(newName)) => name + " as " + newName
                            case (name, _)             => name
                          }
                        )
                        .mkString("Final schema column:\n - ", "\n - ", "")
                      logger.debug(schemaMessage)
                    }

                    //      // Find primary keys
                    val primaryKeysResultSet = use(
                      connectionSettings match {
                        case d if d.isMySQLOrMariaDb() =>
                          databaseMetaData.getPrimaryKeys(
                            schemaName,
                            None.orNull,
                            tableName
                          )
                        case _ =>
                          databaseMetaData.getPrimaryKeys(
                            jdbcSchema.catalog.orNull,
                            schemaName,
                            tableName
                          )
                      }
                    )
                    val primaryKeys = new Iterator[String] {
                      def hasNext: Boolean = primaryKeysResultSet.next()

                      def next(): String = primaryKeysResultSet.getString("COLUMN_NAME")
                    }.toList
                    tableName -> ExtractTableAttributes(
                      tableRemarks,
                      selectedColumns,
                      primaryKeys,
                      currentFilter
                    )
                  }
                } match {
                  case Failure(exception) => throw exception
                  case Success(value)     => value
                }
              }
            }
            .toList
            .toMap
        selectedTablesAndColumnsAndFilter
      } match {
        case Failure(exception) =>
          Utils.logException(logger, exception)
          Map.empty
        case Success(value) => value
      }
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
    logger.debug(s"Interpolating remark $remarks with parameters $parameters")
    val sql = remarks.richFormat(
      parameters,
      Map.empty
    )
    logger.debug(s"Remark interpolated as $sql")
    sql
  }

  /** Create Starlake Domain for JDBC Schema
    *
    * @param jdbcSchema
    * @param domainTemplate
    * @param selectedTablesAndColumns
    * @return
    */
  def extractDomain(
    jdbcSchema: JDBCSchema,
    domainTemplate: Option[Domain],
    selectedTablesAndColumns: Map[String, ExtractTableAttributes]
  ): Domain = {

    def isNumeric(sparkType: String): Boolean = {
      sparkType match {
        case "double" | "decimal" | "long" => true
        case _                             => false
      }
    }

    val trimTemplate =
      domainTemplate.flatMap(_.tables.headOption.flatMap(_.attributes.head.trim))

    val cometSchema = selectedTablesAndColumns.map { case (tableName, tableAttrs) =>
      val sanitizedTableName = Utils.keepAlphaNum(tableName)
      Schema(
        name = tableName,
        rename = if (sanitizedTableName != tableName) Some(sanitizedTableName) else None,
        pattern = Pattern.compile(s"$tableName.*"),
        attributes = tableAttrs.columNames.map(attr =>
          attr.copy(trim =
            if (isNumeric(attr.`type`)) jdbcSchema.numericTrim.orElse(trimTemplate)
            else trimTemplate
          )
        ),
        metadata = None,
        comment = Option(tableAttrs.tableRemarks),
        presql = Nil,
        postsql = Nil,
        primaryKey = tableAttrs.primaryKeys
      )
    }

    // Generate the domain with a dummy watch directory
    val database = domainTemplate.flatMap(_.database)
    val incomingDir = domainTemplate
      .flatMap { dom =>
        dom.resolveDirectoryOpt().map { dir =>
          DatasetArea
            .substituteDomainAndSchemaInPath(
              jdbcSchema.schema,
              "",
              dir
            )
            .toString
        }
      }

    val normalizedDomainName = Utils.keepAlphaNum(jdbcSchema.schema)
    val rename = domainTemplate
      .flatMap(_.rename)
      .map { name =>
        DatasetArea.substituteDomainAndSchema(jdbcSchema.schema, "", name)
      }
      .orElse(
        if (normalizedDomainName != jdbcSchema.schema) Some(normalizedDomainName) else None
      )
    val ack = domainTemplate.flatMap(_.resolveAck())

    Domain(
      database = database,
      name = jdbcSchema.sanitizeName match {
        case Some(true) => Utils.keepAlphaNum(jdbcSchema.schema)
        case _          => jdbcSchema.schema
      },
      rename = rename,
      metadata = domainTemplate
        .flatMap(_.metadata)
        .map(_.copy(directory = incomingDir, ack = ack)),
      tables = cometSchema.toList,
      comment = None
    )
  }

  /** Map jdbc type to spark type (aka Starlake primitive type)
    */
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
        logger.warn(
          s"""Extracting user defined type $colTypename of type $sqlType ($jdbcType) in $tableName.$colName  as string"""
        )
        // colTypename
        "string"
    }
  }

  def jdbcOptions(jdbcOptions: Map[String, String], sparkFormat: String) = {
    val options = if (sparkFormat == "snowflake") {
      jdbcOptions.flatMap { case (k, v) =>
        if (k.startsWith("sf")) {
          val jdbcK = k.replace("sf", "").toLowerCase().replace("database", "db")
          val finalv =
            if (jdbcK == "url")
              "jdbc:snowflake://" + v
            else
              v
          List(
            jdbcK -> finalv,
            k     -> v
          )
        } else
          List(k -> v)

      }
    } else
      jdbcOptions
    CaseInsensitiveMap[String](options)
  }

  def getCommonJDBCType(dt: DataType): Option[JdbcType] = {
    dt match {
      case IntegerType    => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType       => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType     => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType      => Option(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType      => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType       => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType    => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType     => Option(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType     => Option(JdbcType("BLOB", java.sql.Types.BLOB))
      case CharType(n)    => Option(JdbcType(s"CHAR($n)", java.sql.Types.CHAR))
      case VarcharType(n) => Option(JdbcType(s"VARCHAR($n)", java.sql.Types.VARCHAR))
      case TimestampType  => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      // This is a common case of timestamp without time zone. Most of the databases either only
      // support TIMESTAMP type or use TIMESTAMP as an alias for TIMESTAMP WITHOUT TIME ZONE.
      // Note that some dialects override this setting, e.g. as SQL Server.
      case TimestampNTZType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType         => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType =>
        Option(JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _ => None
    }
  }
}

object LastExportUtils extends LazyLogging {

  sealed trait BoundType {
    def value: Any
  }
  case class InclusiveBound(value: Any) extends BoundType
  case class ExclusiveBound(value: Any) extends BoundType
  case class Boundary(lower: BoundType, upper: BoundType)

  case class Bounds(
    firstExport: Boolean,
    typ: PrimitiveType,
    count: Long,
    max: Any,
    partitions: List[Boundary]
  )

  private val MIN_TS = Timestamp.valueOf("1970-01-01 00:00:00")
  private val MIN_DATE = java.sql.Date.valueOf("1970-01-01")
  private val MIN_DECIMAL = java.math.BigDecimal.ZERO

  def getBoundaries(
    conn: SQLConnection,
    auditConnection: SQLConnection,
    extractConfig: ExtractDataConfig,
    tableExtractDataConfig: PartitionnedTableExtractDataConfig,
    auditColumns: Columns
  )(implicit settings: Settings): Bounds = {
    val partitionRange = 0 until tableExtractDataConfig.nbPartitions
    tableExtractDataConfig.partitionColumnType match {
      case PrimitiveType.long | PrimitiveType.short | PrimitiveType.int =>
        val lastExport =
          if (tableExtractDataConfig.fullExport)
            None
          else
            getMaxLongFromSuccessfulExport(
              auditConnection,
              extractConfig,
              tableExtractDataConfig,
              "last_long",
              auditColumns
            )
        internalBoundaries(conn, extractConfig, tableExtractDataConfig, None) { statement =>
          statement.setLong(1, lastExport.getOrElse(Long.MinValue))
          executeQuery(statement) { rs =>
            rs.next()
            val count = rs.getLong(1)
            val (min, max) = {
              tableExtractDataConfig.partitionColumnType match {
                case PrimitiveType.long => (rs.getLong(2), rs.getLong(3))
                case PrimitiveType.int  => (rs.getInt(2).toLong, rs.getInt(3).toLong)
                case _                  => (rs.getShort(2).toLong, rs.getShort(3).toLong)
              }
            }

            val interval = (max - min) / tableExtractDataConfig.nbPartitions
            val intervals = partitionRange.map { index =>
              val lower =
                if (index == 0) InclusiveBound(min) else ExclusiveBound(min + (interval * index))
              val upper =
                if (index == tableExtractDataConfig.nbPartitions - 1)
                  max
                else
                  min + (interval * (index + 1))
              Boundary(lower, InclusiveBound(upper))
            }.toList
            Bounds(
              lastExport.isEmpty,
              PrimitiveType.long,
              count,
              max,
              intervals
            )
          }
        }

      case PrimitiveType.decimal =>
        val lastExport =
          if (tableExtractDataConfig.fullExport)
            None
          else
            getMaxDecimalFromSuccessfulExport(
              auditConnection,
              extractConfig,
              tableExtractDataConfig,
              "last_decimal",
              auditColumns
            )
        internalBoundaries(conn, extractConfig, tableExtractDataConfig, None) { statement =>
          statement.setBigDecimal(1, lastExport.getOrElse(MIN_DECIMAL))
          executeQuery(statement) { rs =>
            rs.next()
            val count = rs.getLong(1)
            val min = Option(rs.getBigDecimal(2)).getOrElse(MIN_DECIMAL)
            val max = Option(rs.getBigDecimal(3)).getOrElse(MIN_DECIMAL)
            val interval = max
              .subtract(min)
              .divide(new java.math.BigDecimal(tableExtractDataConfig.nbPartitions))
            val intervals = partitionRange.map { index =>
              val lower =
                if (index == 0) InclusiveBound(min)
                else ExclusiveBound(min.add(interval.multiply(new java.math.BigDecimal(index))))
              val upper =
                if (index == tableExtractDataConfig.nbPartitions - 1)
                  max
                else
                  min.add(interval.multiply(new java.math.BigDecimal(index + 1)))
              Boundary(lower, InclusiveBound(upper))
            }.toList
            Bounds(
              lastExport.isEmpty,
              PrimitiveType.decimal,
              count,
              max,
              intervals
            )
          }
        }

      case PrimitiveType.date =>
        val lastExport =
          if (tableExtractDataConfig.fullExport)
            None
          else
            getMaxDateFromSuccessfulExport(
              auditConnection,
              extractConfig,
              tableExtractDataConfig,
              "last_date",
              auditColumns
            )
        internalBoundaries(conn, extractConfig, tableExtractDataConfig, None) { statement =>
          statement.setDate(1, lastExport.getOrElse(MIN_DATE))
          executeQuery(statement) { rs =>
            rs.next()
            val count = rs.getLong(1)
            val min = Option(rs.getDate(2)).getOrElse(MIN_DATE)
            val max = Option(rs.getDate(3)).getOrElse(MIN_DATE)
            val interval = (max.getTime() - min.getTime()) / tableExtractDataConfig.nbPartitions
            val intervals = partitionRange.map { index =>
              val lower =
                if (index == 0) InclusiveBound(min)
                else ExclusiveBound(new java.sql.Date(min.getTime() + (interval * index)))
              val upper =
                if (index == tableExtractDataConfig.nbPartitions - 1)
                  max
                else
                  new java.sql.Date(min.getTime() + (interval * (index + 1)))
              Boundary(lower, InclusiveBound(upper))
            }.toList
            Bounds(
              lastExport.isEmpty,
              PrimitiveType.date,
              count,
              max,
              intervals
            )
          }
        }

      case PrimitiveType.timestamp =>
        val lastExport =
          if (tableExtractDataConfig.fullExport)
            None
          else
            getMaxTimestampFromSuccessfulExport(
              auditConnection,
              extractConfig,
              tableExtractDataConfig,
              "last_ts",
              auditColumns
            )
        internalBoundaries(conn, extractConfig, tableExtractDataConfig, None) { statement =>
          statement.setTimestamp(1, lastExport.getOrElse(MIN_TS))
          executeQuery(statement) { rs =>
            rs.next()
            val count = rs.getLong(1)
            val min = Option(rs.getTimestamp(2)).getOrElse(MIN_TS)
            val max = Option(rs.getTimestamp(3)).getOrElse(MIN_TS)
            val interval = (max.getTime() - min.getTime()) / tableExtractDataConfig.nbPartitions
            val intervals = partitionRange.map { index =>
              val lower =
                if (index == 0) InclusiveBound(min)
                else ExclusiveBound(new java.sql.Timestamp(min.getTime() + (interval * index)))
              val upper =
                if (index == tableExtractDataConfig.nbPartitions - 1)
                  max
                else
                  new java.sql.Timestamp(min.getTime() + (interval * (index + 1)))
              Boundary(lower, InclusiveBound(upper))
            }.toList
            Bounds(
              lastExport.isEmpty,
              PrimitiveType.timestamp,
              count,
              max,
              intervals
            )
          }
        }
      case PrimitiveType.string if tableExtractDataConfig.hashFunc.isDefined =>
        if (!tableExtractDataConfig.fullExport) {
          logger.warn(
            "Delta fetching is not compatible with partition on string. Going to extract fully in parallel. To disable this warning please set fullExport in the table definition."
          )
        }
        val stringPartitionFunc = tableExtractDataConfig.hashFunc.map(
          Utils.parseJinjaTpl(
            _,
            Map(
              "col" -> s"${extractConfig.data.quoteIdentifier(tableExtractDataConfig.partitionColumn)}",
              "nb_partitions" -> tableExtractDataConfig.nbPartitions.toString
            )
          )
        )
        internalBoundaries(
          conn,
          extractConfig,
          tableExtractDataConfig,
          stringPartitionFunc
        ) { statement =>
          val (count, min, max) = statement.getParameterMetaData.getParameterType(1) match {
            case java.sql.Types.BIGINT =>
              statement.setLong(1, Long.MinValue)
              executeQuery(statement) { rs =>
                rs.next()
                val count = rs.getLong(1)
                // The algorithm to fetch data doesn't support null values so putting 0 as default value is OK.
                val min: Long = Option(rs.getLong(2)).getOrElse(0)
                val max: Long = Option(rs.getLong(3)).getOrElse(0)
                (count, min, max)
              }
            case java.sql.Types.INTEGER =>
              statement.setInt(1, Int.MinValue)
              executeQuery(statement) { rs =>
                rs.next()
                val count = rs.getLong(1)
                // The algorithm to fetch data doesn't support null values so putting 0 as default value is OK.
                val min: Long = Option(rs.getInt(2)).getOrElse(0).toLong
                val max: Long = Option(rs.getInt(3)).getOrElse(0).toLong
                (count, min, max)
              }
            case java.sql.Types.SMALLINT =>
              statement.setShort(1, Short.MinValue)
              executeQuery(statement) { rs =>
                rs.next()
                val count = rs.getLong(1)
                // The algorithm to fetch data doesn't support null values so putting 0 as default value is OK.
                val min: Long = Option(rs.getShort(2)).getOrElse(0.shortValue()).toLong
                val max: Long = Option(rs.getShort(3)).getOrElse(0.shortValue()).toLong
                (count, min, max)
              }
            case _ =>
              val typeName = statement.getParameterMetaData.getParameterTypeName(1)
              throw new RuntimeException(s"Type $typeName not supported for partition")
          }
          count match {
            case 0 =>
              Bounds(
                firstExport = true,
                PrimitiveType.long,
                count,
                0,
                List.empty
              )
            case _ =>
              val partitions =
                (min until max).map(p => Boundary(InclusiveBound(p), ExclusiveBound(p + 1))).toList
              Bounds(
                firstExport = true,
                PrimitiveType.long,
                count,
                max,
                partitions
              )
          }
        }
      case PrimitiveType.string if tableExtractDataConfig.hashFunc.isEmpty =>
        throw new Exception(
          s"Unsupported type ${tableExtractDataConfig.partitionColumnType} for column partition column ${tableExtractDataConfig.partitionColumn} in table ${tableExtractDataConfig.domain}.${tableExtractDataConfig.table}. You may define your own hash to int function via stringPartitionFunc in jdbcSchema in order to support parallel fetch. Eg: abs( hashtext({{col}}) % {{nb_partitions}} )"
        )
      case _ =>
        throw new Exception(
          s"Unsupported type ${tableExtractDataConfig.partitionColumnType} for column partition column ${tableExtractDataConfig.partitionColumn} in table ${tableExtractDataConfig.domain}.${tableExtractDataConfig.table}"
        )
    }

  }

  private def internalBoundaries[T](
    conn: SQLConnection,
    extractConfig: ExtractDataConfig,
    tableExtractDataConfig: PartitionnedTableExtractDataConfig,
    hashFunc: Option[String]
  )(apply: PreparedStatement => T): T = {
    val extraCondition = tableExtractDataConfig.filterOpt.map(w => s"and $w").getOrElse("")
    val quotedColumn = extractConfig.data.quoteIdentifier(tableExtractDataConfig.partitionColumn)
    val columnToDistribute = hashFunc.getOrElse(quotedColumn)
    val SQL_BOUNDARIES_VALUES =
      s"""select count($quotedColumn) as count_value, min($columnToDistribute) as min_value, max($columnToDistribute) as max_value
         |from ${extractConfig.data.quoteIdentifier(
          tableExtractDataConfig.domain
        )}.${extractConfig.data.quoteIdentifier(tableExtractDataConfig.table)}
         |where $columnToDistribute > ? $extraCondition""".stripMargin
    val preparedStatement = conn.prepareStatement(SQL_BOUNDARIES_VALUES)
    apply(preparedStatement)
  }

  def getMaxLongFromSuccessfulExport(
    conn: SQLConnection,
    extractConfig: ExtractDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    columnName: String,
    auditColumns: Columns
  )(implicit settings: Settings): Option[Long] = getMaxValueFromSuccessfulExport(
    conn,
    extractConfig,
    tableExtractDataConfig,
    columnName,
    auditColumns,
    _.getLong(1)
  )

  def getMaxDecimalFromSuccessfulExport(
    conn: SQLConnection,
    extractConfig: ExtractDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    columnName: String,
    auditColumns: Columns
  )(implicit settings: Settings): Option[java.math.BigDecimal] = getMaxValueFromSuccessfulExport(
    conn,
    extractConfig,
    tableExtractDataConfig,
    columnName,
    auditColumns,
    _.getBigDecimal(1)
  )

  def getMaxTimestampFromSuccessfulExport(
    conn: SQLConnection,
    extractConfig: ExtractDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    columnName: String,
    auditColumns: Columns
  )(implicit settings: Settings): Option[Timestamp] = getMaxValueFromSuccessfulExport(
    conn,
    extractConfig,
    tableExtractDataConfig,
    columnName,
    auditColumns,
    _.getTimestamp(1)
  )

  def getMaxDateFromSuccessfulExport(
    conn: SQLConnection,
    extractConfig: ExtractDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    columnName: String,
    auditColumns: Columns
  )(implicit settings: Settings): Option[Date] = getMaxValueFromSuccessfulExport(
    conn,
    extractConfig,
    tableExtractDataConfig,
    columnName,
    auditColumns,
    _.getDate(1)
  )

  private def getMaxValueFromSuccessfulExport[T](
    conn: SQLConnection,
    extractConfig: ExtractDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    columnName: String,
    auditColumns: Columns,
    extractColumn: ResultSet => T
  )(implicit settings: Settings): Option[T] = {
    val auditSchema = settings.appConfig.audit.getDomain()
    def getColName(colName: String): String = extractConfig.audit.quoteIdentifier(
      getCaseInsensitiveColumnName(auditSchema, lastExportTableName, auditColumns, colName)
    )
    val normalizedColumnName = getColName(columnName)
    val domainColumn = getColName("domain")
    val schemaColumn = getColName("schema")
    val stepColumn = getColName("step")
    val successColumn = getColName("success")
    val lastExtractionSQL =
      s"""
         |select max($normalizedColumnName)
         |  from $auditSchema.$lastExportTableName
         |where
         |  $domainColumn = ?
         |  and $schemaColumn = ?
         |  and $stepColumn = ?
         |  and $successColumn""".stripMargin
    logger.debug(lastExtractionSQL)
    val preparedStatement = conn.prepareStatement(lastExtractionSQL)
    preparedStatement.setString(1, tableExtractDataConfig.domain)
    preparedStatement.setString(2, tableExtractDataConfig.table)
    preparedStatement.setString(3, "ALL")
    val rs = preparedStatement.executeQuery()
    if (rs.next()) {
      val output = extractColumn(rs)
      if (rs.wasNull()) None else Option(output)
    } else {
      None
    }
  }

  def insertNewLastExport(
    conn: SQLConnection,
    row: DeltaRow,
    partitionColumnType: Option[PrimitiveType],
    connectionSettings: Connection,
    auditColumns: Columns
  )(implicit settings: Settings): Int = {
    conn.setAutoCommit(true)
    val auditSchema = settings.appConfig.audit.getDomain()
    def getColName(colName: String): String = connectionSettings.quoteIdentifier(
      getCaseInsensitiveColumnName(auditSchema, lastExportTableName, auditColumns, colName)
    )
    val cols = List(
      "domain",
      "schema",
      "start_ts",
      "end_ts",
      "duration",
      "mode",
      "count",
      "success",
      "message",
      "step"
    ).map(getColName).mkString(",")
    val fullReport =
      s"""insert into $auditSchema.$lastExportTableName($cols) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    val sqlInsert =
      partitionColumnType match {
        case None | Some(PrimitiveType.string) =>
          // For string, we don't support delta. Inserting last value have no sense.
          fullReport
        case Some(partitionColumnType) =>
          val lastExportColumn = partitionColumnType match {
            case PrimitiveType.int | PrimitiveType.long | PrimitiveType.short =>
              getColName("last_long")
            case PrimitiveType.decimal   => getColName("last_decimal")
            case PrimitiveType.date      => getColName("last_date")
            case PrimitiveType.timestamp => getColName("last_ts")
            case _ =>
              throw new Exception(
                s"type $partitionColumnType not supported for partition columnToDistribute"
              )
          }
          val auditSchema = settings.appConfig.audit.getDomain()
          s"""insert into $auditSchema.$lastExportTableName($cols, $lastExportColumn) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
      }

    val preparedStatement = conn.prepareStatement(sqlInsert)
    preparedStatement.setString(1, row.domain)
    preparedStatement.setString(2, row.schema)
    preparedStatement.setTimestamp(3, row.start)
    preparedStatement.setTimestamp(4, row.end)
    preparedStatement.setInt(5, row.duration)
    preparedStatement.setString(6, WriteMode.OVERWRITE.toString)
    preparedStatement.setLong(7, row.count)
    preparedStatement.setBoolean(8, row.success)
    preparedStatement.setString(9, row.message)
    preparedStatement.setString(10, row.step)
    partitionColumnType match {
      case None =>
      case Some(partitionColumnType) =>
        partitionColumnType match {
          case PrimitiveType.int | PrimitiveType.long | PrimitiveType.short =>
            preparedStatement.setLong(11, row.lastExport.asInstanceOf[Long])
          case PrimitiveType.decimal =>
            preparedStatement.setBigDecimal(11, row.lastExport.asInstanceOf[java.math.BigDecimal])
          case PrimitiveType.date =>
            preparedStatement.setDate(11, row.lastExport.asInstanceOf[java.sql.Date])
          case PrimitiveType.timestamp =>
            preparedStatement.setTimestamp(11, row.lastExport.asInstanceOf[java.sql.Timestamp])
          case PrimitiveType.string => // do nothing. If we encounter string here, it means we have succesfully partitionned on it previously.
          case _ =>
            throw new Exception(
              s"type $partitionColumnType not supported for partition columnToDistribute"
            )
        }
    }

    preparedStatement.executeUpdate()
  }

  private def executeQuery[T](
    stmt: PreparedStatement
  )(apply: ResultSet => T): T = {
    val rs = stmt.executeQuery()
    val result = apply(rs)
    rs.close()
    stmt.close()
    result
  }

  private def getCaseInsensitiveColumnName(
    domain: String,
    table: String,
    columns: Columns,
    columnName: String
  ): String = {
    columns
      .find(_.name.equalsIgnoreCase(columnName))
      .map(_.name)
      .getOrElse(
        throw new RuntimeException(s"Column $columnName not found in $domain.$table")
      )
  }

}

case class DeltaRow(
  domain: String,
  schema: String,
  lastExport: Any,
  start: java.sql.Timestamp,
  end: java.sql.Timestamp,
  duration: Int,
  count: Long,
  success: Boolean,
  message: String,
  step: String
)
