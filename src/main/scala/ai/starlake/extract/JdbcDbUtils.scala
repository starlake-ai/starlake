package ai.starlake.extract

import ai.starlake.config.Settings.Connection
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.exceptions.DataExtractionException
import ai.starlake.extract.JdbcDbUtils.{lastExportTableName, Columns}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.{SparkUtils, Utils}
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import com.univocity.parsers.conversions.Conversions
import com.univocity.parsers.csv.{CsvFormat, CsvRoutines, CsvWriterSettings}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import java.nio.charset.StandardCharsets
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
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern
import scala.collection.{mutable, GenTraversable}
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
      "driver class not found in JDBC connection options"
    )
    Class.forName(connectionOptions("driver"))
    val url = connectionOptions("url")
    val properties = new Properties()
    (connectionOptions - "url").foreach { case (key, value) =>
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
      statement.execute(script)
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
    table: String
  )(implicit
    settings: Settings
  ): Option[String] = {
    jdbcSchema.tableRemarks.map { remarks =>
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
    table: String
  )(implicit
    settings: Settings
  ): Option[Map[TableRemarks, TableRemarks]] = {
    jdbcSchema.columnRemarks.map { remarks =>
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
      case d if d.isMySQL() =>
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
  ): Map[TableName, (TableRemarks, Columns, PrimaryKeys)] =
    withJDBCConnection(connectionSettings.options) { connection =>
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
            case d if d.isMySQL() =>
              databaseMetaData.getTables(
                schemaName,
                "%",
                "%",
                jdbcSchema.tableTypes.toArray
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
                        extractTableRemarks(jdbcSchema, connection, tableName)
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
        // Extract the Comet Schema
        val selectedTablesAndColumns: Map[TableName, (TableRemarks, Columns, PrimaryKeys)] =
          ExtractUtils
            .makeParallel(selectedTables)
            .map { case (tableName, tableRemarks) =>
              val jdbcTableColumnsOpt =
                jdbcSchema.tables.find(_.name.equalsIgnoreCase(tableName)).map(_.columns)
              ExtractUtils.timeIt(s"Table extraction of $tableName") {
                logger.info(s"Extracting table $tableName: $tableRemarks")
                withJDBCConnection(connectionSettings.options) { tableExtractConnection =>
                  val databaseMetaData = tableExtractConnection.getMetaData
                  Using.Manager { use =>
                    // Find all foreign keys

                    val foreignKeysResultSet = use(
                      connectionSettings match {
                        case d if d.isMySQL() =>
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
                    val foreignKeys = new Iterator[(String, String)] {
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

                    // Extract all columns
                    val columnsResultSet = use(
                      connectionSettings match {
                        case d if d.isMySQL() =>
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
                        extractColumnRemarks(jdbcSchema, connectionSettings, tableName)
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

                    // Limit to the columns specified by the user if any
                    val currentTableRequestedColumns: Map[ColumnName, Option[ColumnName]] =
                      jdbcTableMap
                        .get(tableName.toUpperCase)
                        .map(_.columns.map(c => c.name.toUpperCase.trim -> c.rename))
                        .getOrElse(Map.empty)
                        .toMap
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
                        case d if d.isMySQL() =>
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
                    tableName -> (tableRemarks, selectedColumns, primaryKeys)
                  }
                } match {
                  case Failure(exception) => throw exception
                  case Success(value)     => value
                }
              }
            }
            .seq
            .toMap
        selectedTablesAndColumns
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
    selectedTablesAndColumns: Map[String, (TableRemarks, Columns, PrimaryKeys)]
  ): Domain = {

    def isNumeric(sparkType: String): Boolean = {
      sparkType match {
        case "double" | "decimal" | "long" => true
        case _                             => false
      }
    }

    val trimTemplate =
      domainTemplate.flatMap(_.tables.headOption.flatMap(_.attributes.head.trim))

    val cometSchema = selectedTablesAndColumns.map {
      case (tableName, (tableRemarks, selectedColumns, primaryKeys)) =>
        val sanitizedTableName = Utils.keepAlphaNum(tableName)
        Schema(
          name = tableName,
          rename = if (sanitizedTableName != tableName) Some(sanitizedTableName) else None,
          pattern = Pattern.compile(s"$tableName.*"),
          attributes = selectedColumns.map(attr =>
            attr.copy(trim =
              if (isNumeric(attr.`type`)) jdbcSchema.numericTrim.orElse(trimTemplate)
              else trimTemplate
            )
          ),
          metadata = None,
          comment = Option(tableRemarks),
          presql = Nil,
          postsql = Nil,
          primaryKey = primaryKeys
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

  /** Extract data and save to output directory
    *
    * @param schemaHandler
    * @param jdbcSchema
    *   All tables referencede here will be saved
    * @param connectionOptions
    *   jdbc connection options for the schema. specific conneciton options may be specified at the
    *   table level.
    * @param baseOutputDir
    *   data is saved in this directory. suffixed by datetime and parition index if any
    * @param limit
    *   For dev mode, it may be useful to extract only a subset of the data
    * @param separator
    *   data are saved as CSV files. this is the separator to use.
    * @param defaultNumPartitions
    *   Parallelism level for the extraction process
    * @param parallelism
    *   number of thread used during extraction
    * @param fullExportCli
    *   fullExport flag coming from cli. Has higher precedence than config files.
    * @param settings
    */
  def extractData(
    extractConfig: ExtractDataConfig
  )(implicit
    settings: Settings,
    schemaHandler: SchemaHandler
  ): Unit = {
    val auditColumns = createExportAuditSchemaIfNotExists(extractConfig.audit)

    // Some database accept strange chars (aka DB2). We get rid of them
    val domainName = extractConfig.jdbcSchema.sanitizeName match {
      case Some(true) => Utils.keepAlphaNum(extractConfig.jdbcSchema.schema)
      case _          => extractConfig.jdbcSchema.schema
    }
    val tableOutputDir = createDomainOutputDir(extractConfig.baseOutputDir, domainName)

    val filteredJdbcSchema: JDBCSchema =
      filterTablesToExtract(extractConfig.jdbcSchema, extractConfig.includeTables)

    // with includeSchemas we may have tables empty. If empty, by default we fetch all tables.
    // This is the short-circuit.
    val doTablesExtraction =
      extractConfig.jdbcSchema.tables.isEmpty || filteredJdbcSchema.tables.nonEmpty
    if (doTablesExtraction) {
      // Map tables to columns and primary keys
      implicit val forkJoinTaskSupport = ExtractUtils.createForkSupport(extractConfig.parallelism)
      val selectedTablesAndColumns: Map[TableName, (TableRemarks, Columns, PrimaryKeys)] =
        JdbcDbUtils.extractJDBCTables(
          filteredJdbcSchema.copy(exclude = extractConfig.excludeTables.toList),
          extractConfig.data,
          skipRemarks = true,
          keepOriginalName = true
        )
      val globalStart = System.currentTimeMillis()
      val extractionResults: List[Try[Unit]] =
        ExtractUtils
          .makeParallel(selectedTablesAndColumns)
          .map { case (tableName, (tableRemarks, selectedColumns, primaryKeys)) =>
            Try {
              val context = s"[${extractConfig.jdbcSchema.schema}.$tableName]"

              // Get the current table partition column and  connection options if any
              val currentTableDefinition =
                getCurrentTableDefinition(extractConfig.jdbcSchema, tableName)
              val currentTableConnectionOptions =
                currentTableDefinition.map(_.connectionOptions).getOrElse(Map.empty)
              // get cols to extract and frame colums names with quotes to handle cols that are keywords in the target database
              val fullExport = extractConfig.fullExport ||
                currentTableDefinition
                  .flatMap(_.fullExport)
                  .orElse(extractConfig.jdbcSchema.fullExport)
                  .getOrElse(
                    false
                  ) // should not happen since fillWithDefaultValues should be called and have false as default one

              val fetchSize =
                currentTableDefinition
                  .flatMap(_.fetchSize)
                  .orElse(extractConfig.jdbcSchema.fetchSize)
              val maybeBartitionColumn = currentTableDefinition
                .flatMap(_.partitionColumn)
                .orElse(extractConfig.jdbcSchema.partitionColumn)
              val tableExtractDataConfig = {
                maybeBartitionColumn match {
                  case Some(partitionColumn) =>
                    val numPartitions = currentTableDefinition
                      .flatMap { tbl =>
                        tbl.numPartitions
                      }
                      .orElse(extractConfig.jdbcSchema.numPartitions)
                      .getOrElse(extractConfig.numPartitions)
                    // Partition column type is useful in order to know how to compare values since comparing numeric, big decimal, date and timestamps are not the same
                    val partitionColumnType = selectedColumns
                      .find(_.name.equalsIgnoreCase(partitionColumn))
                      .flatMap(attr => schemaHandler.types().find(_.name == attr.`type`))
                      .map(_.primitiveType)
                      .getOrElse(
                        throw new Exception(
                          s"Could not find column type for partition column $partitionColumn in table $domainName.$tableName"
                        )
                      )
                    val stringPartitionFuncTpl =
                      extractConfig.jdbcSchema.stringPartitionFunc.orElse(
                        getStringPartitionFunc(extractConfig.data.getJdbcEngineName().toString)
                      )
                    PartitionnedTableExtractDataConfig(
                      domainName,
                      tableName,
                      selectedColumns,
                      fullExport,
                      fetchSize,
                      partitionColumn,
                      partitionColumnType,
                      stringPartitionFuncTpl,
                      numPartitions,
                      tableOutputDir
                    )
                  case None =>
                    UnpartitionnedTableExtractDataConfig(
                      domainName,
                      tableName,
                      selectedColumns,
                      fullExport,
                      fetchSize,
                      tableOutputDir
                    )
                }
              }

              if (
                isExtractionNeeded(
                  extractConfig,
                  tableExtractDataConfig,
                  auditColumns
                )
              ) {
                if (extractConfig.cleanOnExtract) {
                  logger.info(s"Deleting all files of $tableName")
                  tableOutputDir.list
                    .filter(f =>
                      s"^$tableName-\\d{14}[\\.\\-].*".r.pattern.matcher(f.name).matches()
                    )
                    .foreach { f =>
                      f.delete(swallowIOExceptions = true)
                      logger.debug(f"${f.pathAsString} deleted")
                    }
                }

                tableExtractDataConfig match {
                  case _: UnpartitionnedTableExtractDataConfig =>
                    extractTableData(
                      extractConfig.copy(data =
                        extractConfig.data.mergeOptionsWith(currentTableConnectionOptions)
                      ),
                      tableExtractDataConfig,
                      context,
                      auditColumns
                    )
                  case config: PartitionnedTableExtractDataConfig =>
                    extractTablePartionnedData(
                      extractConfig,
                      config,
                      context,
                      auditColumns
                    )
                }
              } else {
                logger.info(s"Extraction skipped. $domainName.$tableName data is fresh enough.")
                Success(())
              }
            }.flatten
          }
          .toList
      forkJoinTaskSupport.foreach(_.forkJoinPool.shutdown())
      val elapsedTime = ExtractUtils.toHumanElapsedTimeFrom(globalStart)
      val nbFailures = extractionResults.count(_.isFailure)
      val dataExtractionFailures = extractionResults
        .flatMap {
          case Failure(e: DataExtractionException) => Some(s"${e.domain}.${e.table}")
          case _                                   => None
        }
        .mkString(", ")
      nbFailures match {
        case 0 =>
          logger.info(s"Extracted sucessfully all tables in $elapsedTime")
        case nb if extractConfig.ignoreExtractionFailure && dataExtractionFailures.nonEmpty =>
          logger.warn(s"Failed to extract $nb tables: $dataExtractionFailures")
        case nb if dataExtractionFailures.nonEmpty =>
          throw new RuntimeException(s"Failed to extract $nb tables: $dataExtractionFailures")
        case nb =>
          throw new RuntimeException(s"Encountered $nb failures during extraction")
      }
    } else {
      logger.info("Tables extraction skipped")
    }
  }

  private def isExtractionNeeded(
    extractDataConfig: ExtractDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    auditColumns: Columns
  )(implicit settings: Settings) = {
    extractDataConfig.extractionPredicate
      .flatMap { predicate =>
        withJDBCConnection(extractDataConfig.audit.options) { connection =>
          LastExportUtils.getLastSuccessfulAllExport(
            connection,
            extractDataConfig,
            tableExtractDataConfig,
            auditColumns
          )
        }.map(t => predicate(t.getTime))
      }
      .getOrElse(true)
  }

  private def getCurrentTableDefinition(jdbcSchema: JDBCSchema, tableName: TableName) = {
    jdbcSchema.tables
      .flatMap { table =>
        if (table.name.contains("*") || table.name.equalsIgnoreCase(tableName)) {
          Some(table)
        } else {
          None
        }
      }
      .sortBy(
        // Table with exact name has precedence over *
        _.name.equalsIgnoreCase(tableName)
      )(Ordering.Boolean.reverse)
      .headOption
  }

  private def extractTablePartionnedData(
    extractConfig: ExtractDataConfig,
    tableExtractDataConfig: PartitionnedTableExtractDataConfig,
    context: String,
    auditColumns: Columns
  )(implicit
    settings: Settings,
    schemaHandler: SchemaHandler,
    forkJoinTaskSupport: Option[ForkJoinTaskSupport]
  ): Try[Unit] = {
    // Table is partitioned, we only extract part of it. Actually, we need to export everything
    // that has not been exported based on the last exported value present in the audit log.

    // This is applied when the table is exported for the first time

    val dataColumnsProjection = tableExtractDataConfig.columnsProjectionQuery(extractConfig.data)

    /** @param columnExprToDistribute
      *   expression to use in order to distribute data.
      */
    def sqlFirst(columnExprToDistribute: String) =
      s"""select $dataColumnsProjection
         |from ${extractConfig.data.quoteIdentifier(
          tableExtractDataConfig.domain
        )}.${extractConfig.data.quoteIdentifier(tableExtractDataConfig.table)}
         |where $columnExprToDistribute <= ?""".stripMargin

    /** @param columnExprToDistribute
      *   expression to use in order to distribute data.
      */
    def sqlNext(columnExprToDistribute: String) =
      s"""select $dataColumnsProjection
         |from ${extractConfig.data.quoteIdentifier(
          tableExtractDataConfig.domain
        )}.${extractConfig.data.quoteIdentifier(tableExtractDataConfig.table)}
         |where $columnExprToDistribute <= ? and $columnExprToDistribute > ?""".stripMargin

    // Get the boundaries of each partition that will be handled by a specific thread.
    val boundaries = withJDBCConnection(extractConfig.data.options) { connection =>
      def getBoundariesWith(auditConnection: SQLConnection) = {
        auditConnection.setAutoCommit(false)
        LastExportUtils.getBoundaries(
          connection,
          auditConnection,
          extractConfig,
          tableExtractDataConfig,
          auditColumns
        )
      }

      if (extractConfig.data.options == extractConfig.audit.options) {
        getBoundariesWith(connection)
      } else {
        withJDBCConnection(extractConfig.audit.options) { auditConnection =>
          getBoundariesWith(auditConnection)
        }
      }
    }

    logger.info(s"$context Boundaries : $boundaries")
    val tableStart = System.currentTimeMillis()
    // Export in parallel mode
    var tableCount = new AtomicLong();
    val extractionResults: GenTraversable[Try[Int]] =
      ExtractUtils.makeParallel(boundaries.partitions.zipWithIndex).map { case (bounds, index) =>
        Try {
          val boundaryContext = s"$context[$index]"
          logger.info(s"$boundaryContext (lower, upper) bounds = $bounds")
          val quotedPartitionColumn =
            extractConfig.data.quoteIdentifier(tableExtractDataConfig.partitionColumn)

          def sql(
            columnToDistribute: String = quotedPartitionColumn
          ) = if (boundaries.firstExport && index == 0) sqlFirst(columnToDistribute)
          else
            sqlNext(columnToDistribute)

          withJDBCConnection(extractConfig.data.options) { connection =>
            val (effectiveSql, statementFiller) = tableExtractDataConfig.partitionColumnType match {
              case PrimitiveType.int | PrimitiveType.long | PrimitiveType.short =>
                val (lower, upper) = bounds.asInstanceOf[(Long, Long)]
                sql() -> ((st: PreparedStatement) => {
                  st.setLong(1, upper)
                  if (!(boundaries.firstExport && index == 0)) st.setLong(2, lower)
                })
              case PrimitiveType.decimal =>
                val (lower, upper) =
                  bounds.asInstanceOf[(java.math.BigDecimal, java.math.BigDecimal)]
                sql() -> ((st: PreparedStatement) => {
                  st.setBigDecimal(1, upper)
                  if (!(boundaries.firstExport && index == 0))
                    st.setBigDecimal(2, lower)
                })

              case PrimitiveType.date =>
                val (lower, upper) = bounds.asInstanceOf[(Date, Date)]
                sql() -> ((st: PreparedStatement) => {
                  st.setDate(1, upper)
                  if (!(boundaries.firstExport && index == 0)) st.setDate(2, lower)
                })

              case PrimitiveType.timestamp =>
                val (lower, upper) =
                  bounds.asInstanceOf[(Timestamp, Timestamp)]
                sql() -> ((st: PreparedStatement) => {
                  st.setTimestamp(1, upper)
                  if (!(boundaries.firstExport && index == 0))
                    st.setTimestamp(2, lower)
                })
              case PrimitiveType.string if tableExtractDataConfig.hashFunc.isDefined =>
                tableExtractDataConfig.hashFunc match {
                  case Some(tpl) =>
                    val stringPartitionFunc =
                      Utils.parseJinjaTpl(
                        tpl,
                        Map(
                          "col"           -> quotedPartitionColumn,
                          "nb_partitions" -> tableExtractDataConfig.nbPartitions.toString
                        )
                      )
                    val (lower, upper) = bounds.asInstanceOf[(Long, Long)]
                    sql(stringPartitionFunc) -> ((st: PreparedStatement) => {
                      st.setLong(1, upper)
                      if (!(boundaries.firstExport && index == 0)) st.setLong(2, lower)
                    })
                  case None =>
                    throw new RuntimeException(
                      "Should never happen since stringPartitionFuncTpl is always defined here"
                    )
                }
              case _ =>
                throw new Exception(
                  s"type ${tableExtractDataConfig.partitionColumnType} not supported for partition columnToDistribute"
                )
            }
            logger.info(s"$boundaryContext SQL: $effectiveSql")
            val partitionStart = System.currentTimeMillis()
            connection.setAutoCommit(false)
            val statement = connection.prepareStatement(effectiveSql)
            statementFiller(statement)
            tableExtractDataConfig.fetchSize.foreach(fetchSize => statement.setFetchSize(fetchSize))

            val count = extractPartitionToFile(
              extractConfig,
              tableExtractDataConfig,
              boundaryContext,
              Some(index),
              statement
            ) match {
              case Failure(exception) =>
                logger.error(f"$boundaryContext Encountered an error during extraction.")
                Utils.logException(logger, exception)
                throw exception
              case Success(value) =>
                value
            }
            val currentTableCount = tableCount.addAndGet(count)

            val lineLength = 100
            val progressPercent =
              if (boundaries.count == 0) lineLength
              else (currentTableCount * lineLength / boundaries.count).toInt
            val progressPercentFilled = (0 until progressPercent).map(_ => "#").mkString
            val progressPercentUnfilled =
              (progressPercent until lineLength).map(_ => " ").mkString
            val progressBar =
              s"[$progressPercentFilled$progressPercentUnfilled] $progressPercent %"
            val partitionEnd = System.currentTimeMillis()
            val elapsedTime = ExtractUtils.toHumanElapsedTimeFrom(tableStart)
            logger.info(
              s"$context $progressBar. Elapsed time: $elapsedTime"
            )
            val deltaRow = DeltaRow(
              domain = extractConfig.jdbcSchema.schema,
              schema = tableExtractDataConfig.table,
              lastExport = boundaries.max,
              start = new Timestamp(partitionStart),
              end = new Timestamp(partitionEnd),
              duration = (partitionEnd - partitionStart).toInt,
              mode = extractConfig.jdbcSchema.writeMode().toString,
              count = count,
              success = true,
              message = tableExtractDataConfig.partitionColumn,
              step = index.toString
            )
            withJDBCConnection(extractConfig.audit.options) { connection =>
              LastExportUtils.insertNewLastExport(
                connection,
                deltaRow,
                Some(tableExtractDataConfig.partitionColumnType),
                extractConfig.audit,
                auditColumns
              )
            }
          }
        }.recoverWith { case _: Exception =>
          Failure(
            new DataExtractionException(
              extractConfig.jdbcSchema.schema,
              tableExtractDataConfig.table
            )
          )
        }
      }
    val success = if (extractionResults.exists(_.isFailure)) {
      logger.error(s"$context An error occured during extraction.")
      extractionResults.foreach {
        case Failure(exception) =>
          Utils.logException(logger, exception)
        case Success(_) => // do nothing
      }
      false
    } else {
      true
    }
    val tableEnd = System.currentTimeMillis()
    val duration = (tableEnd - tableStart).toInt
    val elapsedTime = ExtractUtils.toHumanElapsedTime(duration)
    logger.info(s"$context Extracted all lines in $elapsedTime")
    val deltaRow = DeltaRow(
      domain = extractConfig.jdbcSchema.schema,
      schema = tableExtractDataConfig.table,
      lastExport = boundaries.max,
      start = new Timestamp(tableStart),
      end = new Timestamp(tableEnd),
      duration = duration,
      mode = extractConfig.jdbcSchema.writeMode().toString,
      count = boundaries.count,
      success = success,
      message = tableExtractDataConfig.partitionColumn,
      step = "ALL"
    )
    withJDBCConnection(extractConfig.audit.options) { connection =>
      LastExportUtils.insertNewLastExport(
        connection,
        deltaRow,
        Some(tableExtractDataConfig.partitionColumnType),
        extractConfig.audit,
        auditColumns
      )
    }
    if (success)
      Success(())
    else
      Failure(new RuntimeException(s"$context An error occured during extraction."))
  }

  private def extractTableData(
    extractConfig: ExtractDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    context: String,
    auditColumns: Columns
  )(implicit settings: Settings): Try[Unit] = {
    val dataColumnsProjection = tableExtractDataConfig.columnsProjectionQuery(extractConfig.data)
    // non partitioned tables are fully extracted there is no delta mode
    val sql =
      s"""select $dataColumnsProjection from ${extractConfig.data.quoteIdentifier(
          extractConfig.jdbcSchema.schema
        )}.${extractConfig.data.quoteIdentifier(tableExtractDataConfig.table)}"""
    val tableStart = System.currentTimeMillis()
    val (count, success) = Try {
      withJDBCConnection(extractConfig.data.options) { connection =>
        connection.setAutoCommit(false)
        val statement = connection.prepareStatement(sql)
        tableExtractDataConfig.fetchSize.foreach(fetchSize => statement.setFetchSize(fetchSize))
        logger.info(s"$context Fetch size = ${statement.getFetchSize}")
        logger.info(s"$context SQL: $sql")
        // Export the whole table now
        extractPartitionToFile(
          extractConfig,
          tableExtractDataConfig,
          context,
          None,
          statement
        )
      }
    }.flatten match {
      case Success(count) =>
        count -> true
      case Failure(e) =>
        logger.error(s"$context An error occured during extraction.")
        Utils.logException(logger, e)
        -1L -> false
    }
    val tableEnd = System.currentTimeMillis()
    // Log the extraction in the audit database
    val deltaRow = DeltaRow(
      domain = extractConfig.jdbcSchema.schema,
      schema = tableExtractDataConfig.table,
      lastExport = tableStart,
      start = new Timestamp(tableStart),
      end = new Timestamp(tableEnd),
      duration = (tableEnd - tableStart).toInt,
      mode = extractConfig.jdbcSchema.writeMode().toString,
      count = count,
      success = count >= 0,
      message = "FULL",
      step = "ALL"
    )
    withJDBCConnection(extractConfig.audit.options) { connection =>
      LastExportUtils.insertNewLastExport(
        connection,
        deltaRow,
        None,
        extractConfig.audit,
        auditColumns
      )
    }
    if (success)
      Success(())
    else
      Failure(
        new DataExtractionException(extractConfig.jdbcSchema.schema, tableExtractDataConfig.table)
      )
  }

  private def filterTablesToExtract(jdbcSchema: JDBCSchema, includeTables: Seq[TableName]) = {
    val lowerCasedIncludeTables = includeTables.map(_.toLowerCase)
    val filteredJdbcSchema = if (lowerCasedIncludeTables.nonEmpty) {
      val additionalTables = jdbcSchema.tables.find(_.name.trim == "*") match {
        case Some(allTableDef) =>
          lowerCasedIncludeTables
            .diff(jdbcSchema.tables.map(_.name.toLowerCase))
            .map(n => allTableDef.copy(name = n))
        case None =>
          if (jdbcSchema.tables.isEmpty) {
            lowerCasedIncludeTables.map(JDBCTable(_, Nil, None, None, Map.empty, None, None))
          } else {
            Nil
          }
      }
      val tablesToFetch =
        jdbcSchema.tables.filter(t =>
          lowerCasedIncludeTables.contains(t.name.toLowerCase)
        ) ++ additionalTables
      jdbcSchema.copy(tables = tablesToFetch)
    } else {
      jdbcSchema
    }
    filteredJdbcSchema
  }

  private def createDomainOutputDir(baseOutputDir: File, domainName: TableName): File = {
    baseOutputDir.createDirectories()
    val outputDir = File(baseOutputDir, domainName)
    outputDir.createDirectories()
    outputDir
  }

  private def createExportAuditSchemaIfNotExists(
    connectionSettings: Connection
  )(implicit settings: Settings): Columns = {
    withJDBCConnection(connectionSettings.options) { connection =>
      val auditSchema = settings.appConfig.audit.domain.getOrElse("audit")
      val existLastExportTable =
        tableExists(connection, connectionSettings.jdbcUrl, s"${auditSchema}.$lastExportTableName")
      if (!existLastExportTable && settings.appConfig.createSchemaIfNotExists) {
        createSchema(connection, auditSchema)
        val jdbcEngineName = connectionSettings.getJdbcEngineName()
        settings.appConfig.jdbcEngines.get(jdbcEngineName.toString).foreach { jdbcEngine =>
          val createTableSql = jdbcEngine
            .tables("extract")
            .createSql
            .richFormat(
              Map(
                "table"       -> s"$auditSchema.$lastExportTableName",
                "writeFormat" -> settings.appConfig.defaultWriteFormat
              ),
              Map.empty
            )
          execute(createTableSql, connection)
        }
      }
      JdbcDbUtils
        .extractJDBCTables(
          JDBCSchema(
            schema = auditSchema,
            tables = List(
              JDBCTable(name = lastExportTableName, List(), None, None, Map.empty, None, None)
            ),
            tableTypes = List("TABLE")
          ),
          connectionSettings,
          skipRemarks = true,
          keepOriginalName = true
        )(settings, None)
        .find { case (tableName, _) =>
          tableName.equalsIgnoreCase(lastExportTableName)
        }
        .map { case (_, (_, columns, _)) =>
          columns
        }
        .getOrElse(
          throw new RuntimeException(s"$lastExportTableName table not found. Please create it.")
        )
    }
  }

  private def getStringPartitionFunc(dbType: String): Option[String] = {
    val hashFunctions = Map(
      "sqlserver"  -> "abs( binary_checksum({{col}}) % {{nb_partitions}} )",
      "postgresql" -> "abs( hashtext({{col}}) % {{nb_partitions}} )",
      "h2"         -> "ora_hash({{col}}, {{nb_partitions}} - 1)",
      "mysql"      -> "crc32({{col}}) % {{nb_partitions}}",
      "oracle"     -> "ora_hash({{col}}, {{nb_partitions}} - 1)"
    )
    hashFunctions.get(dbType)
  }

  private def extractPartitionToFile(
    extractConfig: ExtractDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    context: String,
    index: Option[Int],
    statement: PreparedStatement
  ): Try[Long] = {
    val filename = index
      .map(index =>
        tableExtractDataConfig.table + s"-${tableExtractDataConfig.extractionDateTime}-$index.csv"
      )
      .getOrElse(
        tableExtractDataConfig.table + s"-${tableExtractDataConfig.extractionDateTime}.csv"
      )
    val outFile = File(tableExtractDataConfig.tableOutputDir, filename)
    Try {
      logger.info(s"$context Starting extraction into $filename")
      val outFileWriter = outFile.newFileOutputStream(append = false)
      val writerSettings = new CsvWriterSettings()
      val format = new CsvFormat()
      extractConfig.outputFormat.quote.flatMap(_.headOption).foreach { q =>
        format.setQuote(q)
      }
      extractConfig.outputFormat.escape.flatMap(_.headOption).foreach(format.setQuoteEscape)
      extractConfig.outputFormat.separator.foreach(format.setDelimiter)
      writerSettings.setFormat(format)
      extractConfig.outputFormat.nullValue.foreach(writerSettings.setNullValue)
      extractConfig.outputFormat.withHeader.foreach(writerSettings.setHeaderWritingEnabled)
      val csvRoutines = new CsvRoutines(writerSettings)

      statement.setMaxRows(extractConfig.limit)
      val rs = statement.executeQuery()
      val extractionStartMs = System.currentTimeMillis()
      val objectRowWriterProcessor = new SLObjectRowWriterProcessor()
      extractConfig.outputFormat.datePattern.foreach(dtp =>
        objectRowWriterProcessor.convertType(classOf[java.sql.Date], Conversions.toDate(dtp))
      )
      extractConfig.outputFormat.timestampPattern.foreach(tp =>
        objectRowWriterProcessor.convertType(classOf[java.sql.Timestamp], Conversions.toDate(tp))
      )
      writerSettings.setQuoteAllFields(true)
      writerSettings.setRowWriterProcessor(objectRowWriterProcessor)
      csvRoutines.write(
        rs,
        outFileWriter,
        extractConfig.outputFormat.encoding.getOrElse(StandardCharsets.UTF_8.name())
      )
      val elapsedTime = ExtractUtils.toHumanElapsedTimeFrom(extractionStartMs)
      logger.info(
        s"$context Extracted ${objectRowWriterProcessor.getRecordsCount()} rows and saved into $filename in $elapsedTime"
      )
      objectRowWriterProcessor.getRecordsCount()
    }.recoverWith { case e =>
      outFile.delete()
      Failure(e)
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
}

object LastExportUtils extends LazyLogging {
  case class Bounds(
    firstExport: Boolean,
    typ: PrimitiveType,
    count: Long,
    min: Any,
    max: Any,
    partitions: Array[(Any, Any)]
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
          lastExportValue(
            auditConnection,
            extractConfig.audit,
            tableExtractDataConfig,
            "last_long",
            auditColumns
          ) { rs =>
            if (rs.next()) {
              val res = rs.getLong(1)
              if (res == 0) None else Some(res) // because null return 0
            } else
              None
          }
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
              val upper =
                if (index == tableExtractDataConfig.nbPartitions - 1)
                  max
                else
                  min + (interval * (index + 1))
              (min + (interval * index), upper)
            }.toArray
            Bounds(
              lastExport.isEmpty,
              PrimitiveType.long,
              count,
              min,
              max,
              intervals.asInstanceOf[Array[(Any, Any)]]
            )
          }
        }

      case PrimitiveType.decimal =>
        val lastExport =
          lastExportValue(
            auditConnection,
            extractConfig.audit,
            tableExtractDataConfig,
            "last_decimal",
            auditColumns
          ) { rs =>
            if (rs.next())
              Some(rs.getBigDecimal(1))
            else
              None
          }
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
              val upper =
                if (index == tableExtractDataConfig.nbPartitions - 1)
                  max
                else
                  min.add(interval.multiply(new java.math.BigDecimal(index + 1)))
              (min.add(interval.multiply(new java.math.BigDecimal(index))), upper)
            }.toArray
            Bounds(
              lastExport.isEmpty,
              PrimitiveType.decimal,
              count,
              min,
              max,
              intervals.asInstanceOf[Array[(Any, Any)]]
            )
          }
        }

      case PrimitiveType.date =>
        val lastExport =
          lastExportValue(
            auditConnection,
            extractConfig.audit,
            tableExtractDataConfig,
            "last_date",
            auditColumns
          ) { rs =>
            if (rs.next())
              Some(rs.getDate(1))
            else
              None
          }
        internalBoundaries(conn, extractConfig, tableExtractDataConfig, None) { statement =>
          statement.setDate(1, lastExport.getOrElse(MIN_DATE))
          executeQuery(statement) { rs =>
            rs.next()
            val count = rs.getLong(1)
            val min = Option(rs.getDate(2)).getOrElse(MIN_DATE)
            val max = Option(rs.getDate(3)).getOrElse(MIN_DATE)
            val interval = (max.getTime() - min.getTime()) / tableExtractDataConfig.nbPartitions
            val intervals = partitionRange.map { index =>
              val upper =
                if (index == tableExtractDataConfig.nbPartitions - 1)
                  max
                else
                  new java.sql.Date(min.getTime() + (interval * (index + 1)))
              (new java.sql.Date(min.getTime() + (interval * index)), upper)
            }.toArray
            Bounds(
              lastExport.isEmpty,
              PrimitiveType.date,
              count,
              min,
              max,
              intervals.asInstanceOf[Array[(Any, Any)]]
            )
          }
        }

      case PrimitiveType.timestamp =>
        val lastExport =
          lastExportValue(
            auditConnection,
            extractConfig.audit,
            tableExtractDataConfig,
            "last_ts",
            auditColumns
          ) { rs =>
            if (rs.next())
              Some(rs.getTimestamp(1))
            else
              None
          }
        internalBoundaries(conn, extractConfig, tableExtractDataConfig, None) { statement =>
          statement.setTimestamp(1, lastExport.getOrElse(MIN_TS))
          executeQuery(statement) { rs =>
            rs.next()
            val count = rs.getLong(1)
            val min = Option(rs.getTimestamp(2)).getOrElse(MIN_TS)
            val max = Option(rs.getTimestamp(3)).getOrElse(MIN_TS)
            val interval = (max.getTime() - min.getTime()) / tableExtractDataConfig.nbPartitions
            val intervals = partitionRange.map { index =>
              val upper =
                if (index == tableExtractDataConfig.nbPartitions - 1)
                  max
                else
                  new java.sql.Timestamp(min.getTime() + (interval * (index + 1)))
              (new java.sql.Timestamp(min.getTime() + (interval * index)), upper)
            }.toArray
            Bounds(
              lastExport.isEmpty,
              PrimitiveType.timestamp,
              count,
              min,
              max,
              intervals.asInstanceOf[Array[(Any, Any)]]
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
          statement.setLong(1, Long.MinValue)
          executeQuery(statement) { rs =>
            rs.next()
            val count = rs.getLong(1)
            // The algorithm to fetch data doesn't support null values so putting 0 as default value is OK.
            val min: Long = Option(rs.getLong(2)).getOrElse(0)
            val max: Long = Option(rs.getLong(3)).getOrElse(0)
            count match {
              case 0 =>
                Bounds(
                  firstExport = true,
                  PrimitiveType.long,
                  count,
                  0,
                  0,
                  Array.empty[(Any, Any)]
                )
              case _ =>
                val partitions = (min until max).map(p => p -> (p + 1)).toArray
                Bounds(
                  firstExport = true,
                  PrimitiveType.long,
                  count,
                  min,
                  max,
                  partitions.asInstanceOf[Array[(Any, Any)]]
                )
            }
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

  private def lastExportValue[T](
    conn: SQLConnection,
    connectionSettings: Connection,
    tableExtractDataConfig: TableExtractDataConfig,
    columnName: String,
    auditColumns: Columns
  )(apply: ResultSet => Option[T])(implicit settings: Settings): Option[T] = {
    val auditSchema = settings.appConfig.audit.getDomain()
    if (tableExtractDataConfig.fullExport) {
      None
    } else {
      def getColName(colName: String): String = connectionSettings.quoteIdentifier(
        getCaseInsensitiveColumnName(auditSchema, lastExportTableName, auditColumns, colName)
      )
      val SQL_LAST_EXPORT_VALUE =
        s"""select max(${getColName(columnName)}) from $auditSchema.$lastExportTableName
           |where ${getColName("domain")} like ? and ${getColName("schema")} like ?""".stripMargin
      val preparedStatement = conn.prepareStatement(SQL_LAST_EXPORT_VALUE)
      preparedStatement.setString(1, tableExtractDataConfig.domain)
      preparedStatement.setString(2, tableExtractDataConfig.table)
      executeQuery(preparedStatement)(apply)
    }
  }

  private def internalBoundaries[T](
    conn: SQLConnection,
    extractConfig: ExtractDataConfig,
    tableExtractDataConfig: PartitionnedTableExtractDataConfig,
    hashFunc: Option[String]
  )(apply: PreparedStatement => T): T = {
    val quotedColumn = extractConfig.data.quoteIdentifier(tableExtractDataConfig.partitionColumn)
    val columnToDistribute = hashFunc.getOrElse(quotedColumn)
    val SQL_BOUNDARIES_VALUES =
      s"""select count($quotedColumn) as count_value, min($columnToDistribute) as min_value, max($columnToDistribute) as max_value
         |from ${extractConfig.data.quoteIdentifier(
          tableExtractDataConfig.domain
        )}.${extractConfig.data.quoteIdentifier(tableExtractDataConfig.table)}
         |where $columnToDistribute > ?""".stripMargin
    val preparedStatement = conn.prepareStatement(SQL_BOUNDARIES_VALUES)
    apply(preparedStatement)
  }

  def getLastSuccessfulAllExport(
    conn: SQLConnection,
    extractConfig: ExtractDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    auditColumns: Columns
  )(implicit settings: Settings): Option[Timestamp] = {
    val auditSchema = settings.appConfig.audit.getDomain()
    def getColName(colName: String): String = extractConfig.audit.quoteIdentifier(
      getCaseInsensitiveColumnName(auditSchema, lastExportTableName, auditColumns, colName)
    )
    val startTsColumn = getColName("start_ts")
    val domainColumn = getColName("domain")
    val schemaColumn = getColName("schema")
    val stepColumn = getColName("step")
    val successColumn = getColName("success")
    val lastExtractionSQL =
      s"""
         |select max($startTsColumn)
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
      Option(rs.getTimestamp(1))
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
    preparedStatement.setString(6, row.mode)
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
  mode: String,
  count: Long,
  success: Boolean,
  message: String,
  step: String
)
