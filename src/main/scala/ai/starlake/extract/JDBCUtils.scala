package ai.starlake.extract

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.Utils
import better.files.File
import com.typesafe.scalalogging.LazyLogging

import java.io.FileWriter
import java.sql.Types._
import java.sql.{
  Connection => SQLConnection,
  Date,
  DriverManager,
  PreparedStatement,
  ResultSet,
  Timestamp
}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
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

  /** Execute a block of code in the context of a newly created connection. We better use here a
    * Connection pool, but since starlake processes are short lived, we do not really need it.
    * @param connectionOptions
    * @param f
    * @param settings
    * @tparam T
    * @return
    */
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

  /** RUn the sql statement in the context of a connection
    * @param script
    * @param connectionOptions
    * @param settings
    * @return
    */
  def applyScript(script: String, connectionOptions: Map[String, String])(implicit
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

  /** Get column comments for a database table
    * @param jdbcSchema
    * @param connectionOptions
    * @param table
    * @param settings
    * @return
    */
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

  /** Get all tables, columns and primary keys of a database schema as described in the YML file.
    * Exclude tables not selected in the YML file
    * @param jdbcSchema
    * @param connectionOptions
    * @param settings
    * @return
    */
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
                columns
              else
                columns.filter(col => currentTableRequestedColumns.contains(col.name.toUpperCase()))
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
    val incomingDir = domainTemplate
      .map { dom =>
        DatasetArea
          .substituteDomainAndSchemaInPath(
            jdbcSchema.schema,
            "",
            dom.resolveDirectory()
          )
          .toString
      }
      .getOrElse(s"/${jdbcSchema.schema}")

    val normalizedDomainName = Utils.keepAlphaNum(jdbcSchema.schema)
    val rename = domainTemplate
      .flatMap(_.rename)
      .map { name =>
        DatasetArea.substituteDomainAndSchema(jdbcSchema.schema, "", name)
      }
      .orElse(
        if (normalizedDomainName != jdbcSchema.schema) Some(normalizedDomainName) else None
      )
    val extensions = domainTemplate.map(_.resolveExtensions()).getOrElse(Nil)
    val ack = domainTemplate.flatMap(_.resolveAck())

    Domain(
      name = jdbcSchema.schema,
      rename = rename,
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
    * @param settings
    */
  def extractData(
    schemaHandler: SchemaHandler,
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    baseOutputDir: File,
    limit: Int,
    separator: String,
    defaultNumPartitions: Int,
    clean: Boolean
  )(implicit
    settings: Settings
  ): Unit = {
    val auditConnectionOptions =
      settings.comet.connections.get("audit").map(_.options).getOrElse(connectionOptions)

    // Some database accept strange chars (aka DB2). We get rid of them
    val domainName = jdbcSchema.schema.replaceAll("[^\\p{Alnum}]", "_")
    baseOutputDir.createDirectories()
    val outputDir = File(baseOutputDir, domainName)
    outputDir.createDirectories()
    if (clean)
      outputDir.list.foreach(_.delete(swallowIOExceptions = true))

    // Map tables to columns and primary keys
    val selectedTablesAndColumns: Map[String, (TableRemarks, Columns, PrimaryKeys)] =
      JDBCUtils.extractJDBCTables(jdbcSchema, connectionOptions)

    selectedTablesAndColumns.foreach {
      case (tableName, (tableRemarks, selectedColumns, primaryKeys)) =>
        val formatter = DateTimeFormatter
          .ofPattern("yyyyMMddHHmmss")
          .withZone(ZoneId.systemDefault())
        val dateTime = formatter.format(Instant.now())

        // get cols to extract and frame colums names with quotes to handle cols that are keywords in the target database
        val cols = selectedColumns.map('"' + _.name + '"').mkString(",")

        // We always add header line to the export with the column names
        val header = selectedColumns.map(_.name).mkString(separator)

        // Get the current table partition column and  connection options if any
        val currentTable = jdbcSchema.tables.find(_.name.equalsIgnoreCase(tableName))
        val currentTableConnectionOptions =
          currentTable.map(_.connectionOptions).getOrElse(Map.empty)

        val partitionColumn = currentTable
          .flatMap(_.partitionColumn)
          .orElse(jdbcSchema.partitionColumn)

        val fetchSize =
          jdbcSchema.fetchSize.orElse(currentTable.flatMap(_.fetchSize))

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
            val start = System.currentTimeMillis()
            val count = Try {
              withJDBCConnection(connectionOptions ++ currentTableConnectionOptions) { connection =>
                connection.setAutoCommit(false)
                val statement = connection.prepareStatement(sql)
                fetchSize.foreach(fetchSize => statement.setFetchSize(fetchSize))
                logger.info(s"SQL: $sql")
                // Export the whole table now
                extractPartitionToFile(
                  limit,
                  separator,
                  outputDir,
                  tableName,
                  dateTime,
                  None,
                  statement,
                  header
                )
              }
            } match {
              case Success(count) =>
                count
              case Failure(e) =>
                Utils.logException(logger, e)
                -1
            }
            val end = System.currentTimeMillis()
            // Log the extraction in the audit database
            val deltaRow = DeltaRow(
              domain = jdbcSchema.schema,
              schema = tableName,
              lastExport = start,
              start = new Timestamp(start),
              end = new Timestamp(end),
              duration = (end - start).toInt,
              mode = jdbcSchema.writeMode().toString,
              count = count,
              success = count >= 0,
              message = "FULL",
              step = "ALL"
            )
            withJDBCConnection(auditConnectionOptions) { connection =>
              LastExportUtils.insertNewLastExport(connection, deltaRow, None)
            }

          case Some(partitionColumn) =>
            // Table is partitioned, we only extract part of it. Actually, we need to export everything
            // that has not been exported based on the last exported value present in the audit log.

            // This is applied when the table is exported for the first time
            val sqlFirst =
              s"""select $cols
                 |from ${jdbcSchema.schema}.$tableName
                 |where $partitionColumn <= ?""".stripMargin
            val sqlNext =
              s"""select $cols
                 |from ${jdbcSchema.schema}.$tableName
                 |where $partitionColumn <= ? and $partitionColumn > ?""".stripMargin

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
              connection.setAutoCommit(false)
              LastExportUtils.getBoundaries(
                connection,
                jdbcSchema.schema,
                tableName,
                partitionColumn,
                partitionColumnType,
                numPartitions
              )
            }

            logger.info(s"Boundaries: $boundaries")
            val start = System.currentTimeMillis()
            // Export in parallel mode
            val success = Try {
              boundaries.partitions.zipWithIndex.par.foreach { case (bounds, index) =>
                logger.info(s"(lower, upper) bounds = $bounds")
                val sql = if (boundaries.firstExport && index == 0) sqlFirst else sqlNext
                logger.info(s"SQL: $sql")
                withJDBCConnection(connectionOptions) { connection =>
                  val start = System.currentTimeMillis()
                  connection.setAutoCommit(false)
                  val statement = connection.prepareStatement(sql)
                  fetchSize.foreach(fetchSize => statement.setFetchSize(fetchSize))
                  partitionColumnType match {
                    case PrimitiveType.int | PrimitiveType.long | PrimitiveType.short =>
                      val (lower, upper) = bounds.asInstanceOf[(Long, Long)]
                      statement.setLong(1, upper)
                      if (!(boundaries.firstExport && index == 0)) statement.setLong(2, lower)
                    case PrimitiveType.decimal =>
                      val (lower, upper) =
                        bounds.asInstanceOf[(java.math.BigDecimal, java.math.BigDecimal)]
                      statement.setBigDecimal(1, upper)
                      if (!(boundaries.firstExport && index == 0))
                        statement.setBigDecimal(2, lower)
                    case PrimitiveType.date =>
                      val (lower, upper) = bounds.asInstanceOf[(Date, Date)]
                      statement.setDate(1, upper)
                      if (!(boundaries.firstExport && index == 0)) statement.setDate(2, lower)
                    case PrimitiveType.timestamp =>
                      val (lower, upper) =
                        bounds.asInstanceOf[(Timestamp, Timestamp)]
                      statement.setTimestamp(1, upper)
                      if (!(boundaries.firstExport && index == 0))
                        statement.setTimestamp(2, lower)
                    case _ =>
                      throw new Exception(
                        s"type $partitionColumnType not supported for partition column"
                      )
                  }
                  val count = extractPartitionToFile(
                    limit,
                    separator,
                    outputDir,
                    tableName,
                    dateTime,
                    Some(index),
                    statement,
                    header
                  )
                  val end = System.currentTimeMillis()
                  val deltaRow = DeltaRow(
                    domain = jdbcSchema.schema,
                    schema = tableName,
                    lastExport = boundaries.max,
                    start = new Timestamp(start),
                    end = new Timestamp(end),
                    duration = (end - start).toInt,
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
                      Some(partitionColumnType)
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
            val end = System.currentTimeMillis()
            val deltaRow = DeltaRow(
              domain = jdbcSchema.schema,
              schema = tableName,
              lastExport = boundaries.max,
              start = new Timestamp(start),
              end = new Timestamp(end),
              duration = (end - start).toInt,
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
                Some(partitionColumnType)
              )
            }

        }
    }
  }

  private def extractPartitionToFile(
    limit: Int,
    separator: String,
    outputDir: File,
    tableName: String,
    dateTime: String,
    index: Option[Int],
    statement: PreparedStatement,
    header: String
  ): Long = {
    val filename = index
      .map(index => tableName + s"-$dateTime-$index.csv")
      .getOrElse(tableName + s"-$dateTime.csv")
    val outFile = File(outputDir, filename)
    val outFileWriter = outFile.newFileWriter(append = false)
    val res = extractPartitionToFileWriter(limit, separator, outFileWriter, statement, header)
    outFileWriter.close()
    res
  }

  private def extractPartitionToFileWriter(
    limit: Int,
    separator: String,
    outFileWriter: FileWriter,
    statement: PreparedStatement,
    header: String
  ): Long = {
    outFileWriter.append(header + "\n")
    statement.setMaxRows(limit)
    val rs = statement.executeQuery()
    val rsMetadata = rs.getMetaData()
    var count: Long = 0
    while (rs.next()) {
      count = count + 1
      val colList = ListBuffer.empty[String]
      for (icol <- 1 to rsMetadata.getColumnCount) {
        val obj = Option(rs.getObject(icol))
        val sqlType = rsMetadata.getColumnType(icol)
        import java.sql.Types
        val colValue = sqlType match {
          case Types.VARCHAR =>
            "\"" + rs.getString(icol) + "\""
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
    logger.info(s"Extracted $count rows")
    count
  }
}

object LastExportUtils {
  case class Bounds(
    firstExport: Boolean,
    typ: PrimitiveType,
    count: Long,
    min: Any,
    max: Any,
    partitions: Array[(Any, Any)]
  )

  val MIN_TS = Timestamp.valueOf("1970-01-01 00:00:00")
  val MIN_DATE = java.sql.Date.valueOf("1970-01-01")
  val MIN_DECIMAL = java.math.BigDecimal.ZERO

  def getBoundaries(
    conn: SQLConnection,
    domain: String,
    table: String,
    partitionColumn: String,
    partitionColumnType: PrimitiveType,
    nbPartitions: Int
  ): Bounds = {
    val partitionRange = 0 until nbPartitions
    partitionColumnType match {
      case PrimitiveType.long | PrimitiveType.short | PrimitiveType.int =>
        val lastExport = lastExportValue(conn, domain, table, "last_long") { rs =>
          if (rs.next()) {
            val res = rs.getLong(1)
            if (res == 0) None else Some(res) // because null return 0
          } else
            None
        }
        internalBoundaries(conn, domain, table, partitionColumn) { statement =>
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
        val lastExport = lastExportValue(conn, domain, table, "last_decimal") { rs =>
          if (rs.next())
            Some(rs.getBigDecimal(1))
          else
            None
        }
        internalBoundaries(conn, domain, table, partitionColumn) { statement =>
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
        val lastExport = lastExportValue(conn, domain, table, "last_date") { rs =>
          if (rs.next())
            Some(rs.getDate(1))
          else
            None
        }
        internalBoundaries(conn, domain, table, partitionColumn) { statement =>
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
        val lastExport = lastExportValue(conn, domain, table, "last_ts") { rs =>
          if (rs.next())
            Some(rs.getTimestamp(1))
          else
            None
        }
        internalBoundaries(conn, domain, table, partitionColumn) { statement =>
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
      case _ =>
        throw new Exception(
          s"Unsupported type ${partitionColumnType} for column partition column $partitionColumn in table $domain.$table"
        )
    }

  }

  private def lastExportValue[T](
    conn: SQLConnection,
    domain: String,
    table: String,
    columnName: String
  )(apply: ResultSet => T): T = {
    val SQL_LAST_EXPORT_VALUE =
      s"""select max("$columnName") from SLK_LAST_EXPORT where "domain" like ? and "schema" like ?"""
    val preparedStatement = conn.prepareStatement(SQL_LAST_EXPORT_VALUE)
    preparedStatement.setString(1, domain)
    preparedStatement.setString(2, table)
    executeQuery(preparedStatement)(apply)
  }

  private def internalBoundaries[T](
    conn: SQLConnection,
    domain: String,
    table: String,
    partitionColumn: String
  )(apply: PreparedStatement => T): T = {
    val SQL_BOUNDARIES_VALUES =
      s"""select count("$partitionColumn") as count_value, min("$partitionColumn") as min_value, max("$partitionColumn") as max_value
           |from $domain.$table
           |where $partitionColumn > ?""".stripMargin
    val preparedStatement = conn.prepareStatement(SQL_BOUNDARIES_VALUES)
    apply(preparedStatement)
  }

  def insertNewLastExport(
    conn: SQLConnection,
    row: DeltaRow,
    partitionColumnType: Option[PrimitiveType]
  ): Int = {
    conn.setAutoCommit(true)
    val sqlInsert =
      partitionColumnType match {
        case None =>
          s"""insert into SLK_LAST_EXPORT("domain", "schema", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step") values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        case Some(partitionColumnType) =>
          val lastExportColumn = partitionColumnType match {
            case PrimitiveType.int | PrimitiveType.long | PrimitiveType.short => "last_long"
            case PrimitiveType.decimal                                        => "last_decimal"
            case PrimitiveType.date                                           => "last_date"
            case PrimitiveType.timestamp                                      => "last_ts"
            case _ =>
              throw new Exception(s"type $partitionColumnType not supported for partition column")
          }
          s"""insert into SLK_LAST_EXPORT("domain", "schema", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step", "$lastExportColumn") values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
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
          case _ =>
            throw new Exception(s"type $partitionColumnType not supported for partition column")
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

  def executeUpdate(stmt: PreparedStatement): Try[Int] =
    Try {
      val count = stmt.executeUpdate()
      stmt.close()
      count
    }

  class CountRowsRequest(
    fullTableName: String,
    timestampColumn: String,
    domainName: String,
    schemaName: String,
    lastExportDate: Timestamp,
    newExportDate: Timestamp
  ) {
    val queryString =
      s"""SELECT COUNT(*) FROM "$fullTableName"" WHERE "$timestampColumn" > '$lastExportDate' AND "$timestampColumn" <= '$newExportDate'"""

    def getResult(resultSet: ResultSet): Int = resultSet.getInt(0)
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
