package ai.starlake.extract

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.Utils
import au.com.bytecode.opencsv.CSVWriter
import better.files.{using, File}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.jdbc.JdbcDialects

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
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern
import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.{Failure, Success, Try, Using}

object JdbcDbUtils extends LazyLogging {

  type TableRemarks = String
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

  // The other part of the biMap
  private val reverseSqlTypes = sqlTypes map (_.swap)

  /** Execute a block of code in the context of a newly created connection. We better use here a
    * Connection pool, but since starlake processes are short lived, we do not really need it.
    * @param connectionOptions
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
    val result = Try {
      f(connection)
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

  def tableExists(conn: SQLConnection, url: String, table: String): Boolean = {
    val dialect = JdbcDialects.get(url)
    Try {
      val statement = conn.prepareStatement(dialect.getTableExistsQuery(table))
      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    } match {
      case Failure(e) =>
        logger.warn(s"Table $table does not exist , ${e.getMessage}")
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
      val res = statement.execute(script)
      connection.commit()
      res
    } finally {
      statement.close()
      connection.setAutoCommit(isAutoCommit)
    }
  }

  @throws[Exception]
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

  /** RUn the sql statement in the context of a connection
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
    * @param jdbcSchema
    * @param connectionOptions
    * @param table
    * @param settings
    * @return
    */
  private def extractTableRemarks(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    table: String
  )(implicit
    settings: Settings
  ): Option[String] = {
    jdbcSchema.tableRemarks.map { remarks =>
      val sql = formatRemarksSQL(jdbcSchema, table, remarks)
      logger.debug(s"Extracting table remarks using $sql")
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

  /** Get column comments for a database table
    * @param jdbcSchema
    * @param connectionOptions
    * @param table
    * @param settings
    * @return
    */
  private def extractColumnRemarks(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    table: String
  )(implicit
    settings: Settings
  ): Option[Map[TableRemarks, TableRemarks]] = {
    jdbcSchema.columnRemarks.map { remarks =>
      val sql = formatRemarksSQL(jdbcSchema, table, remarks)
      logger.debug(s"Extracting column remarks using $sql")
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

  private def extractCaseInsensitiveSchemaName(
    databaseMetaData: DatabaseMetaData,
    catalog: Option[String],
    schemaName: String
  ): Try[String] = {

    Using(
      databaseMetaData.getSchemas(
        catalog.orNull,
        "%"
      )
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

  /** Get all tables, columns and primary keys of a database schema as described in the YML file.
    * Exclude tables not selected in the YML file
    * @param jdbcSchema
    * @param connectionOptions
    * @param settings
    * @return
    */
  def extractJDBCTables(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    skipRemarks: Boolean,
    excludeTables: Seq[String] = Nil
  )(implicit
    settings: Settings,
    fjp: Option[ForkJoinTaskSupport]
  ): Map[String, (TableRemarks, Columns, PrimaryKeys)] =
    withJDBCConnection(connectionOptions) { connection =>
      val databaseMetaData = connection.getMetaData()

      val jdbcTableMap =
        jdbcSchema.tables
          .map(tblSchema => tblSchema.name.toUpperCase -> tblSchema)
          .toMap
      val uppercaseTableNames = jdbcTableMap.keys.toList

      /* Extract all tables from the database and return Map of tablename -> tableDescription */
      def extractTables(
        schemaName: String,
        tablesToExtract: List[String] = Nil
      ): Map[String, String] = {
        val tableNames = mutable.Map.empty[String, String]
        Try {
          databaseMetaData.getTables(
            jdbcSchema.catalog.orNull,
            schemaName,
            "%",
            jdbcSchema.tableTypes.toArray
          )
        } match {
          case Success(resultSet) =>
            Using(resultSet) { resultSet =>
              while (resultSet.next()) {
                val tableName = resultSet.getString("TABLE_NAME")
                if (tablesToExtract.isEmpty || tablesToExtract.contains(tableName.toUpperCase())) {
                  val localRemarks =
                    if (skipRemarks) None
                    else extractTableRemarks(jdbcSchema, connectionOptions, tableName)
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
        databaseMetaData,
        jdbcSchema.catalog,
        jdbcSchema.schema
      ).map { schemaName =>
        val selectedTablesBeforeExclusion = uppercaseTableNames match {
          case Nil =>
            extractTables(schemaName)
          case list if list.contains("*") =>
            extractTables(schemaName)
          case list =>
            val extractedTableNames = extractTables(schemaName, list)
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
        val lowerCasedExcludeTables = excludeTables.map(_.toLowerCase)
        val selectedTables = selectedTablesBeforeExclusion.filter { case (tableName, _) =>
          !lowerCasedExcludeTables.contains(tableName.toLowerCase)
        }
        logger.whenDebugEnabled {
          selectedTables.keys.foreach(table => logger.debug(s"Selected: $table"))
        }
        // Extract the Comet Schema
        val selectedTablesAndColumns: Map[String, (TableRemarks, Columns, PrimaryKeys)] =
          ExtractUtils
            .makeParallel(selectedTables)
            .map { case (tableName, tableRemarks) =>
              ExtractUtils.timeIt(s"Table extraction of $tableName") {
                logger.info(s"Extracting table $tableName: $tableRemarks")
                Using.Manager { use =>
                  // Find all foreign keys

                  val foreignKeysResultSet = use(
                    databaseMetaData.getImportedKeys(
                      jdbcSchema.catalog.orNull,
                      schemaName,
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
                      schemaName,
                      tableName,
                      null
                    )
                  )
                  val remarks: Map[TableRemarks, TableRemarks] = (
                    if (skipRemarks) None
                    else
                      extractColumnRemarks(jdbcSchema, connectionOptions, tableName)
                  ).getOrElse(Map.empty)

                  val isPostgres = connectionOptions("url").contains("postgres")
                  val attrs = new Iterator[Attribute] {
                    def hasNext: Boolean = columnsResultSet.next()

                    def next(): Attribute = {
                      val colName = columnsResultSet.getString("COLUMN_NAME")
                      logger.debug(s"COLUMN_NAME=$tableName.$colName")
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
                  val currentTableRequestedColumns =
                    jdbcTableMap
                      .get(tableName)
                      .map(_.columns.map(_.toUpperCase))
                      .getOrElse(Nil)
                  val selectedColumns =
                    if (currentTableRequestedColumns.isEmpty)
                      columns
                    else
                      columns.filter(col =>
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
                      schemaName,
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
            }
            .seq
            .toMap
        selectedTablesAndColumns
      } match {
        case Failure(exception) =>
          Utils.logException(logger, exception)
          Map.empty[String, (TableRemarks, Columns, PrimaryKeys)]
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
          merge = None,
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
      comment = None,
      ack = None
    )
  }

  /** Map jdbc type to spark type (aka Starlake primitive type)
    */
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

  /** @param jdbcConnectionUrl
    *   used to know which database we are targeting.
    */
  private def getColNameQuote(jdbcConnectionUrl: String) = {
    val specialColNameQuote = List(
      "jdbc:mysql" -> '`',
      "jdbc:h2"    -> '`'
    )
    specialColNameQuote
      .find { case (jdbcPrefix, _) => jdbcConnectionUrl.contains(jdbcPrefix) }
      .map { case (_, colNameQuote) => colNameQuote }
      .getOrElse('"')
  }

  /** Extract data and save to output directory
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
    schemaHandler: SchemaHandler,
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    baseOutputDir: File,
    limit: Int,
    separator: Char,
    defaultNumPartitions: Int,
    parallelism: Option[Int],
    fullExportCli: Boolean,
    datePattern: String,
    timestampPattern: String,
    extractionPredicate: Option[Long => Boolean],
    cleanOnExtract: Boolean,
    includeTables: Seq[String],
    excludeTables: Seq[String]
  )(implicit
    settings: Settings
  ): Unit = {
    val auditConnectionOptions =
      settings.appConfig.connections.get("audit").map(_.options).getOrElse(connectionOptions)
    val jdbcUrl = connectionOptions("url")
    // Because mysql does not support "" when framing column names to handle cases where
    // column names are keywords in the target dialect
    val colNameQuoteData = getColNameQuote(jdbcUrl)
    val colNameQuoteAudit = getColNameQuote(auditConnectionOptions("url"))

    // Some database accept strange chars (aka DB2). We get rid of them
    val domainName = jdbcSchema.sanitizeName match {
      case Some(true) => Utils.keepAlphaNum(jdbcSchema.schema)
      case _          => jdbcSchema.schema
    }
    baseOutputDir.createDirectories()
    val outputDir = File(baseOutputDir, domainName)
    outputDir.createDirectories()

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

    // with includeSchemas we may have tables empty. If empty, by default we fetch all tables.
    // This is the short-circuit.
    val doTablesExtraction = jdbcSchema.tables.isEmpty || filteredJdbcSchema.tables.nonEmpty
    if (doTablesExtraction) {
      // Map tables to columns and primary keys
      implicit val forkJoinTaskSupport = ExtractUtils.createForkSupport(parallelism)
      val selectedTablesAndColumns: Map[String, (TableRemarks, Columns, PrimaryKeys)] =
        JdbcDbUtils.extractJDBCTables(
          filteredJdbcSchema,
          connectionOptions,
          skipRemarks = true,
          excludeTables
        )
      val globalStart = System.currentTimeMillis()
      ExtractUtils.makeParallel(selectedTablesAndColumns).foreach {
        case (tableName, (tableRemarks, selectedColumns, primaryKeys)) =>
          val context = s"[${jdbcSchema.schema}.$tableName]"

          // Get the current table partition column and  connection options if any
          val currentTable = jdbcSchema.tables
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
          val currentTableConnectionOptions =
            currentTable.map(_.connectionOptions).getOrElse(Map.empty)

          val doExtraction = extractionPredicate
            .flatMap { predicate =>
              withJDBCConnection(auditConnectionOptions) { connection =>
                LastExportUtils.getLastAllExport(
                  connection,
                  jdbcSchema.schema,
                  tableName,
                  colNameQuoteAudit
                )
              }.map(t => predicate(t.getTime))
            }
            .getOrElse(true)
          if (doExtraction) {
            if (cleanOnExtract) {
              logger.info(s"Deleting all files of $tableName")
              outputDir.list
                .filter(f => s"^$tableName-\\d{14}[\\.\\-].*".r.pattern.matcher(f.name).matches())
                .foreach { f =>
                  f.delete(swallowIOExceptions = true)
                  logger.debug(f"${f.pathAsString} deleted")
                }
            }
            val formatter = DateTimeFormatter
              .ofPattern("yyyyMMddHHmmss")
              .withZone(ZoneId.systemDefault())
            val dateTime = formatter.format(Instant.now())

            // get cols to extract and frame colums names with quotes to handle cols that are keywords in the target database
            val cols =
              selectedColumns.map(colNameQuoteData + _.name + colNameQuoteData).mkString(",")
            val partitionColumn = currentTable
              .flatMap(_.partitionColumn)
              .orElse(jdbcSchema.partitionColumn)

            val fetchSize = currentTable.flatMap(_.fetchSize).orElse(jdbcSchema.fetchSize)

            val fullExport =
              if (fullExportCli) fullExportCli
              else
                currentTable
                  .flatMap(_.fullExport)
                  .orElse(jdbcSchema.fullExport)
                  .getOrElse(
                    false
                  ) // should not happen since fillWithDefaultValues should be called and have false as default one

            val numPartitions = currentTable
              .flatMap { tbl =>
                tbl.numPartitions
              }
              .orElse(jdbcSchema.numPartitions)
              .getOrElse(defaultNumPartitions)

            partitionColumn match {
              case None =>
                // non partitioned tables are fully extracted there is no delta mode
                val sql = s"""select $cols from "${jdbcSchema.schema}"."$tableName""""
                val tableStart = System.currentTimeMillis()
                val count = Try {
                  withJDBCConnection(connectionOptions ++ currentTableConnectionOptions) {
                    connection =>
                      connection.setAutoCommit(false)
                      val statement = connection.prepareStatement(sql)
                      fetchSize.foreach(fetchSize => statement.setFetchSize(fetchSize))
                      logger.info(s"$context Fetch size = ${statement.getFetchSize}")
                      logger.info(s"$context SQL: $sql")
                      // Export the whole table now
                      extractPartitionToFile(
                        context,
                        limit,
                        separator,
                        outputDir,
                        tableName,
                        dateTime,
                        None,
                        statement,
                        datePattern,
                        timestampPattern
                      )
                  }
                } match {
                  case Success(count) =>
                    count
                  case Failure(e) =>
                    Utils.logException(logger, e)
                    -1
                }
                val tableEnd = System.currentTimeMillis()
                // Log the extraction in the audit database
                val deltaRow = DeltaRow(
                  domain = jdbcSchema.schema,
                  schema = tableName,
                  lastExport = tableStart,
                  start = new Timestamp(tableStart),
                  end = new Timestamp(tableEnd),
                  duration = (tableEnd - tableStart).toInt,
                  mode = jdbcSchema.writeMode().toString,
                  count = count,
                  success = count >= 0,
                  message = "FULL",
                  step = "ALL"
                )
                withJDBCConnection(auditConnectionOptions) { connection =>
                  LastExportUtils.insertNewLastExport(
                    connection,
                    deltaRow,
                    None,
                    colNameQuoteAudit,
                    colNameQuoteAudit
                  )
                }

              case Some(partitionColumn) =>
                lazy val stringPartitionFuncTpl =
                  jdbcSchema.stringPartitionFunc.orElse(getStringPartitionFunc(jdbcUrl))
                // Table is partitioned, we only extract part of it. Actually, we need to export everything
                // that has not been exported based on the last exported value present in the audit log.

                // This is applied when the table is exported for the first time
                def sqlFirst(columnToDistribute: String) =
                  s"""select $cols
                     |from $colNameQuoteData${jdbcSchema.schema}$colNameQuoteData.$colNameQuoteData$tableName$colNameQuoteData
                     |where $columnToDistribute <= ?""".stripMargin

                def sqlNext(columnToDistribute: String) =
                  s"""select $cols
                     |from $colNameQuoteData${jdbcSchema.schema}$colNameQuoteData.$colNameQuoteData$tableName$colNameQuoteData
                     |where $columnToDistribute <= ? and $columnToDistribute > ?""".stripMargin

                // Get the partition column type. Since comparing numeric, big decimal, date and timestamps are not the same
                val partitionColumnType =
                  selectedColumns
                    .find(_.name.equalsIgnoreCase(partitionColumn))
                    .flatMap(attr => schemaHandler.types().find(_.name == attr.`type`))
                    .map(_.primitiveType)
                    .getOrElse(
                      throw new Exception(
                        s"Could not find column type for partition column $partitionColumn in table ${jdbcSchema.schema}.$tableName"
                      )
                    )
                // Get the boundaries of each partition that will be handled by a specific thread.
                val boundaries = withJDBCConnection(connectionOptions) { connection =>
                  def getBoundariesWith(auditConnection: SQLConnection) = {
                    auditConnection.setAutoCommit(false)
                    LastExportUtils.getBoundaries(
                      connection,
                      auditConnection,
                      jdbcSchema.schema,
                      tableName,
                      partitionColumn,
                      partitionColumnType,
                      numPartitions,
                      colNameQuoteData,
                      colNameQuoteAudit,
                      stringPartitionFuncTpl,
                      fullExport
                    )
                  }

                  if (connectionOptions == auditConnectionOptions) {
                    getBoundariesWith(connection)
                  } else {
                    withJDBCConnection(auditConnectionOptions) { auditConnection =>
                      getBoundariesWith(auditConnection)
                    }
                  }
                }

                logger.info(s"$context Boundaries : $boundaries")
                val tableStart = System.currentTimeMillis()
                // Export in parallel mode
                var tableCount = new AtomicLong();
                val success = Try {
                  ExtractUtils.makeParallel(boundaries.partitions.zipWithIndex).foreach {
                    case (bounds, index) =>
                      val boundaryContext = s"$context[$index]"
                      logger.info(s"$boundaryContext (lower, upper) bounds = $bounds")
                      val quotedPartitionColumn =
                        s"$colNameQuoteData$partitionColumn$colNameQuoteData"

                      def sql(
                        columnToDistribute: String = quotedPartitionColumn
                      ) = if (boundaries.firstExport && index == 0) sqlFirst(columnToDistribute)
                      else
                        sqlNext(columnToDistribute)

                      withJDBCConnection(connectionOptions) { connection =>
                        val (effectiveSql, statementFiller) = partitionColumnType match {
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
                          case PrimitiveType.string if stringPartitionFuncTpl.isDefined =>
                            stringPartitionFuncTpl match {
                              case Some(tpl) =>
                                val stringPartitionFunc =
                                  Utils.parseJinjaTpl(
                                    tpl,
                                    Map(
                                      "col"           -> quotedPartitionColumn,
                                      "nb_partitions" -> numPartitions.toString
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
                              s"type $partitionColumnType not supported for partition columnToDistribute"
                            )
                        }
                        logger.info(s"$boundaryContext SQL: $effectiveSql")
                        val partitionStart = System.currentTimeMillis()
                        connection.setAutoCommit(false)
                        val statement = connection.prepareStatement(effectiveSql)
                        statementFiller(statement)
                        fetchSize.foreach(fetchSize => statement.setFetchSize(fetchSize))

                        val count = extractPartitionToFile(
                          boundaryContext,
                          limit,
                          separator,
                          outputDir,
                          tableName,
                          dateTime,
                          Some(index),
                          statement,
                          datePattern,
                          timestampPattern
                        )
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
                          domain = jdbcSchema.schema,
                          schema = tableName,
                          lastExport = boundaries.max,
                          start = new Timestamp(partitionStart),
                          end = new Timestamp(partitionEnd),
                          duration = (partitionEnd - partitionStart).toInt,
                          mode = jdbcSchema.writeMode().toString,
                          count = count,
                          success = true,
                          message = partitionColumn,
                          step = index.toString
                        )
                        withJDBCConnection(auditConnectionOptions) { connection =>
                          LastExportUtils.insertNewLastExport(
                            connection,
                            deltaRow,
                            Some(partitionColumnType),
                            colNameQuoteAudit,
                            colNameQuoteAudit
                          )
                        }
                      }
                  }
                } match {
                  case Success(_) =>
                    true
                  case Failure(e) =>
                    Utils.logException(logger, e)
                    false
                }
                val tableEnd = System.currentTimeMillis()
                val duration = (tableEnd - tableStart).toInt
                val elapsedTime = ExtractUtils.toHumanElapsedTime(duration)
                logger.info(s"$context Extracted all lines in $elapsedTime")
                val deltaRow = DeltaRow(
                  domain = jdbcSchema.schema,
                  schema = tableName,
                  lastExport = boundaries.max,
                  start = new Timestamp(tableStart),
                  end = new Timestamp(tableEnd),
                  duration = duration,
                  mode = jdbcSchema.writeMode().toString,
                  count = boundaries.count,
                  success = success,
                  message = partitionColumn,
                  step = "ALL"
                )
                withJDBCConnection(auditConnectionOptions) { connection =>
                  LastExportUtils.insertNewLastExport(
                    connection,
                    deltaRow,
                    Some(partitionColumnType),
                    colNameQuoteAudit,
                    colNameQuoteAudit
                  )
                }

            }
          } else {
            logger.info(s"Extraction skipped. $domainName.$tableName data is fresh enough.")
          }
      }

      forkJoinTaskSupport.foreach(_.forkJoinPool.shutdown())
      val elapsedTime = ExtractUtils.toHumanElapsedTimeFrom(globalStart)
      logger.info(s"Extracted all tables in $elapsedTime")
    } else {
      logger.info("Tables extraction skipped")
    }
  }

  private def getStringPartitionFunc(jdbcUrl: String): Option[String] = {
    val hashFunctions = List(
      "jdbc:sqlserver" -> "abs( binary_checksum({{col}}) % {{nb_partitions}} )",
      "jdbc:postgres"  -> "abs( hashtext({{col}}) % {{nb_partitions}} )",
      "jdbc:h2"        -> "ora_hash({{col}}, {{nb_partitions}} - 1)",
      "jdbc:mysql"     -> "crc32({{col}}) % {{nb_partitions}}",
      "jdbc:oracle"    -> "ora_hash({{col}}, {{nb_partitions}} - 1)"
    )
    hashFunctions.find { case (dbType, _) => jdbcUrl.contains(dbType) }.map {
      case (_, partitionFunc) => partitionFunc
    }
  }

  private def extractPartitionToFile(
    context: String,
    limit: Int,
    separator: Char,
    outputDir: File,
    tableName: String,
    dateTime: String,
    index: Option[Int],
    statement: PreparedStatement,
    datePattern: String,
    timestampPattern: String
  ): Long = {
    val filename = index
      .map(index => tableName + s"-$dateTime-$index.csv")
      .getOrElse(tableName + s"-$dateTime.csv")
    logger.info(s"$context Starting extraction into $filename")
    val outFile = File(outputDir, filename)
    val outFileWriter = outFile.newFileWriter(append = false)
    val csvWriterResource = new CSVWriter(outFileWriter, separator, '"', '\\')
    statement.setMaxRows(limit)
    val rs = statement.executeQuery()
    val resultService = new SLResultSetHelperService(datePattern, timestampPattern)
    var count: Long = 0
    val extractionStartMs = System.currentTimeMillis()
    using(csvWriterResource) { csvWriter =>
      csvWriter.setResultService(resultService)
      csvWriter.writeNext(resultService.getColumnNames(rs))
      val heartBeatMs = 30000
      var lastHeartBeat = extractionStartMs
      while (rs.next()) {
        count = count + 1
        csvWriter.writeNext(resultService.getColumnValues(rs))
        if (System.currentTimeMillis() - lastHeartBeat > heartBeatMs) {
          lastHeartBeat = System.currentTimeMillis()
          val elapsedTime = ExtractUtils.toHumanElapsedTimeFrom(extractionStartMs)
          logger.info(
            s"$context Extraction in progress. Already extracted $count rows in $elapsedTime"
          )
        }
      }
    }
    val elapsedTime = ExtractUtils.toHumanElapsedTimeFrom(extractionStartMs)
    logger.info(s"$context Extracted $count rows and saved into $filename in $elapsedTime")
    count
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
    domain: String,
    table: String,
    partitionColumn: String,
    partitionColumnType: PrimitiveType,
    nbPartitions: Int,
    colNameDataQuote: Char,
    colNameAuditQuote: Char,
    stringPartitionFuncTpl: => Option[String],
    fullExport: Boolean
  )(implicit settings: Settings): Bounds = {
    val partitionRange = 0 until nbPartitions
    partitionColumnType match {
      case PrimitiveType.long | PrimitiveType.short | PrimitiveType.int =>
        val lastExport =
          lastExportValue(
            auditConnection,
            domain,
            table,
            "last_long",
            colNameAuditQuote,
            fullExport
          ) { rs =>
            if (rs.next()) {
              val res = rs.getLong(1)
              if (res == 0) None else Some(res) // because null return 0
            } else
              None
          }
        internalBoundaries(conn, domain, table, partitionColumn, colNameDataQuote, None) {
          statement =>
            statement.setLong(1, lastExport.getOrElse(Long.MinValue))
            executeQuery(statement) { rs =>
              rs.next()
              val count = rs.getLong(1)
              val (min, max) =
                if (partitionColumnType == PrimitiveType.long)
                  (rs.getLong(2), rs.getLong(3))
                else if (partitionColumnType == PrimitiveType.int)
                  (rs.getInt(2).toLong, rs.getInt(3).toLong)
                else // short
                  (rs.getShort(2).toLong, rs.getShort(3).toLong)

              val interval = (max - min) / nbPartitions
              val intervals = partitionRange.map { index =>
                val upper =
                  if (index == nbPartitions - 1)
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
            domain,
            table,
            "last_decimal",
            colNameAuditQuote,
            fullExport
          ) { rs =>
            if (rs.next())
              Some(rs.getBigDecimal(1))
            else
              None
          }
        internalBoundaries(conn, domain, table, partitionColumn, colNameDataQuote, None) {
          statement =>
            statement.setBigDecimal(1, lastExport.getOrElse(MIN_DECIMAL))
            executeQuery(statement) { rs =>
              rs.next()
              val count = rs.getLong(1)
              val min = Option(rs.getBigDecimal(2)).getOrElse(MIN_DECIMAL)
              val max = Option(rs.getBigDecimal(3)).getOrElse(MIN_DECIMAL)
              val interval = max.subtract(min).divide(new java.math.BigDecimal(nbPartitions))
              val intervals = partitionRange.map { index =>
                val upper =
                  if (index == nbPartitions - 1)
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
            domain,
            table,
            "last_date",
            colNameAuditQuote,
            fullExport
          ) { rs =>
            if (rs.next())
              Some(rs.getDate(1))
            else
              None
          }
        internalBoundaries(conn, domain, table, partitionColumn, colNameDataQuote, None) {
          statement =>
            statement.setDate(1, lastExport.getOrElse(MIN_DATE))
            executeQuery(statement) { rs =>
              rs.next()
              val count = rs.getLong(1)
              val min = Option(rs.getDate(2)).getOrElse(MIN_DATE)
              val max = Option(rs.getDate(3)).getOrElse(MIN_DATE)
              val interval = (max.getTime() - min.getTime()) / nbPartitions
              val intervals = partitionRange.map { index =>
                val upper =
                  if (index == nbPartitions - 1)
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
            domain,
            table,
            "last_ts",
            colNameAuditQuote,
            fullExport
          ) { rs =>
            if (rs.next())
              Some(rs.getTimestamp(1))
            else
              None
          }
        internalBoundaries(conn, domain, table, partitionColumn, colNameDataQuote, None) {
          statement =>
            statement.setTimestamp(1, lastExport.getOrElse(MIN_TS))
            executeQuery(statement) { rs =>
              rs.next()
              val count = rs.getLong(1)
              val min = Option(rs.getTimestamp(2)).getOrElse(MIN_TS)
              val max = Option(rs.getTimestamp(3)).getOrElse(MIN_TS)
              val interval = (max.getTime() - min.getTime()) / nbPartitions
              val intervals = partitionRange.map { index =>
                val upper =
                  if (index == nbPartitions - 1)
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
      case PrimitiveType.string if stringPartitionFuncTpl.isDefined =>
        if (!fullExport) {
          logger.warn(
            "Delta fetching is not compatible with partition on string. Going to extract fully in parallel. To disable this warning please set fullExport in the table definition."
          )
        }
        val stringPartitionFunc = stringPartitionFuncTpl.map(
          Utils.parseJinjaTpl(
            _,
            Map(
              "col"           -> s"$colNameDataQuote$partitionColumn$colNameDataQuote",
              "nb_partitions" -> nbPartitions.toString
            )
          )
        )
        internalBoundaries(
          conn,
          domain,
          table,
          partitionColumn,
          colNameDataQuote,
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
                  true,
                  PrimitiveType.long,
                  count,
                  0,
                  0,
                  Array.empty[(Any, Any)]
                )
              case _ =>
                val partitions = (min until max).map(p => p -> (p + 1)).toArray
                Bounds(
                  true,
                  PrimitiveType.long,
                  count,
                  min,
                  max,
                  partitions.asInstanceOf[Array[(Any, Any)]]
                )
            }
          }
        }
      case PrimitiveType.string if stringPartitionFuncTpl.isEmpty =>
        throw new Exception(
          s"Unsupported type $partitionColumnType for column partition column $partitionColumn in table $domain.$table. You may define your own hash to int function via stringPartitionFunc in jdbcSchema in order to support parallel fetch. Eg: abs( hashtext({{col}}) % {{nb_partitions}} )"
        )
      case _ =>
        throw new Exception(
          s"Unsupported type $partitionColumnType for column partition column $partitionColumn in table $domain.$table"
        )
    }

  }

  private def lastExportValue[T](
    conn: SQLConnection,
    domain: String,
    table: String,
    columnName: String,
    colQuote: Char,
    fullExport: Boolean
  )(apply: ResultSet => Option[T])(implicit settings: Settings): Option[T] = {
    val auditSchema = settings.appConfig.audit.domain.getOrElse("audit")
    if (fullExport) {
      None
    } else {
      val SQL_LAST_EXPORT_VALUE =
        s"""select max($colQuote$columnName$colQuote) from $auditSchema.SL_LAST_EXPORT where ${colQuote}domain${colQuote} like ? and ${colQuote}schema${colQuote} like ?"""
      val preparedStatement = conn.prepareStatement(SQL_LAST_EXPORT_VALUE)
      preparedStatement.setString(1, domain)
      preparedStatement.setString(2, table)
      executeQuery(preparedStatement)(apply)
    }
  }

  private def internalBoundaries[T](
    conn: SQLConnection,
    domain: String,
    table: String,
    partitionColumn: String,
    colQuote: Char,
    hashFunc: Option[String]
  )(apply: PreparedStatement => T): T = {
    val quotedColumn = s"$colQuote$partitionColumn$colQuote"
    val columnToDistribute = hashFunc.getOrElse(quotedColumn)
    val SQL_BOUNDARIES_VALUES =
      s"""select count($quotedColumn) as count_value, min($columnToDistribute) as min_value, max($columnToDistribute) as max_value
           |from $colQuote$domain$colQuote.$colQuote$table$colQuote
           |where $columnToDistribute > ?""".stripMargin
    val preparedStatement = conn.prepareStatement(SQL_BOUNDARIES_VALUES)
    apply(preparedStatement)
  }

  def getLastAllExport(
    conn: SQLConnection,
    domain: String,
    schema: String,
    colNameQuote: Char
  )(implicit settings: Settings): Option[Timestamp] = {
    val auditSchema = settings.appConfig.audit.domain.getOrElse("audit")
    val lastExtractionSQL =
      s"""
         |select max(${colNameQuote}start_ts${colNameQuote})
         |  from $auditSchema.SL_LAST_EXPORT
         |where
         |  ${colNameQuote}domain${colNameQuote} = ?
         |  and ${colNameQuote}schema${colNameQuote} = ?
         |  and ${colNameQuote}step${colNameQuote} = ?""".stripMargin
    logger.debug(lastExtractionSQL)
    val preparedStatement = conn.prepareStatement(lastExtractionSQL)
    preparedStatement.setString(1, domain)
    preparedStatement.setString(2, schema)
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
    colStart: Char,
    colEnd: Char
  )(implicit settings: Settings): Int = {
    conn.setAutoCommit(true)
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
    ).map(col => colStart + col + colEnd).mkString(",")
    val auditSchema = settings.appConfig.audit.domain.getOrElse("audit")
    val fullReport =
      s"""insert into $auditSchema.SL_LAST_EXPORT($cols) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    val sqlInsert =
      partitionColumnType match {
        case None | Some(PrimitiveType.string) =>
          // For string, we don't support delta. Inserting last value have no sense.
          fullReport
        case Some(partitionColumnType) =>
          val lastExportColumn = partitionColumnType match {
            case PrimitiveType.int | PrimitiveType.long | PrimitiveType.short => "last_long"
            case PrimitiveType.decimal                                        => "last_decimal"
            case PrimitiveType.date                                           => "last_date"
            case PrimitiveType.timestamp                                      => "last_ts"
            case _ =>
              throw new Exception(
                s"type $partitionColumnType not supported for partition columnToDistribute"
              )
          }
          val auditSchema = settings.appConfig.audit.domain.getOrElse("audit")
          s"""insert into $auditSchema.SL_LAST_EXPORT($cols, $colStart$lastExportColumn$colEnd) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
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
