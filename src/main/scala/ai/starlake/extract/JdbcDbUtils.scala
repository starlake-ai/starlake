package ai.starlake.extract

import ai.starlake.config.Settings.{ConnectionInfo, JdbcEngine}
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.core.utils.StringUtils
import ai.starlake.extract.JdbcDbUtils.{lastExportTableName, Columns}
import ai.starlake.job.Main
import ai.starlake.schema.model.*
import ai.starlake.sql.SQLUtils
import ai.starlake.tests.StarlakeTestData.DomainName
import ai.starlake.utils.{SparkUtils, StarlakeJdbcOps, Utils}
import com.manticore.jsqlformatter.JSQLFormatter
import com.typesafe.scalalogging.LazyLogging
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types.*

import java.sql.{
  Connection,
  DatabaseMetaData,
  Date,
  DriverManager,
  PreparedStatement,
  ResultSet,
  Timestamp
}
import java.util.Properties
import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try, Using}

object JdbcDbUtils extends LazyLogging {
  val appType = Option(System.getenv("SL_API_APP_TYPE")).getOrElse("web")

  type TableName = String
  type TableRemarks = String
  type ColumnName = String
  type Columns = List[TableAttribute]
  type PrimaryKeys = List[String]

  case class SqlColumn(
    name: String,
    dataType: String,
    precision: Option[String],
    scale: Option[String]
  ) {
    def sqlTypeAsString(): String = {
      (precision, scale) match {
        case (Some(p), Some(s)) => s"$dataType($p,$s)"
        case (Some(p), None)    => s"$dataType($p,0)"
        case _                  => dataType
      }
    }
  }

  object StarlakeConnectionPool {
    private val hikariPools = scala.collection.concurrent.TrieMap[String, HikariDataSource]()
    private val duckDbPool = scala.collection.concurrent.TrieMap[String, Connection]()

    private def getHikariPoolKey(url: String, options: Map[String, String]): String = {
      val keysToConsider =
        if (appType == "snowflake_native_app") {
          options.filter { case (k, v) =>
            Set("password", "user", "authenticator").contains(k) && v.nonEmpty
          }
        } else {
          options.filter { case (k, v) =>
            Set("password", "sl_access_token", "user").contains(k) && v.nonEmpty
          }
        }
      url + "?" + keysToConsider.toList.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString("&")
    }
    def getConnection(
      dataBranch: Option[String],
      connectionOptions: Map[String, String]
    ): java.sql.Connection = {
      if (!connectionOptions.contains("driver")) {
        Try(throw new Exception("Driver class not found in JDBC connection options")) match {
          case Failure(exception) =>
            exception.printStackTrace()
          case Success(connection) =>
        }
      }

      assert(
        connectionOptions.contains("driver"),
        s"driver class not found in JDBC connection options $connectionOptions"
      )
      val (driver, url) = StarlakeJdbcOps.driverAndUrl(
        dataBranch,
        connectionOptions("driver"),
        connectionOptions("url")
      )
      if (url.contains(":duckdb:")) {
        // No connection pool for duckdb. This is a single user database on write.
        // We need to release the connection asap
        val properties = new Properties()
        (connectionOptions - "url" - "driver" - "dbtable" - "numpartitions" - "sl_access_token" - "account")
          .foreach { case (k, v) =>
            properties.setProperty(k, v)
          }

        val dbKey = url + properties.toString
        if (!isExtractCommandHack(url)) {
          val sqlConn = DriverManager.getConnection(url, properties)
          sqlConn
        } else {
          duckDbPool.getOrElse(
            dbKey, {
              duckDbPool.find { case (key, value) =>
                key.startsWith(url)
              } match {
                case Some((key, sqlConn)) =>
                  sqlConn.close()
                  duckDbPool.remove(key)
                case None =>
              }
              val sqlConn = DriverManager.getConnection(url, properties)
              duckDbPool.put(dbKey, sqlConn)
              sqlConn
            }
          )
        }
      } else {

        val finalConnectionOptions =
          if (
            url.contains(":snowflake:") &&
            connectionOptions.contains("sl_access_token") &&
            connectionOptions("sl_access_token").contains(":") && appType != "snowflake_native_app"
          ) {
            // this is the case for Snowflake OAuth as a web app not as a native app
            val accountUserAndToken = connectionOptions("sl_access_token").split(":")
            val account = accountUserAndToken(0)
            val user = accountUserAndToken(1)
            val accessToken =
              accountUserAndToken.drop(2).mkString(":") // in case the token contains the ':' char
            connectionOptions
              .updated("authenticator", "oauth")
              .updated("account", account)
              .updated("user", user)
              .updated("password", accessToken)
              .updated("allowUnderscoresInHost", "true")

            // password is the token in Snowflake 3.13+
            // properties.setProperty("user", ...)
            // properties.setProperty("role", ...)
          } else if (url.contains(":snowflake:")) {
            connectionOptions.updated("allowUnderscoresInHost", "true")
          } else {
            connectionOptions
          }

        val javaProperties = new Properties()
        (finalConnectionOptions - "url" - "driver" - "dbtable" - "numpartitions" - "sl_access_token" - "account")
          .foreach { case (k, v) =>
            javaProperties.setProperty(k, v)
          }
        val connection =
          if (System.getenv("SL_USE_CONNECTION_POOLING") == "true") {
            logger.info("Using connection pooling")
            val poolKey = getHikariPoolKey(url, finalConnectionOptions)

            val pool = hikariPools
              .getOrElseUpdate(
                poolKey, {
                  val config = new HikariConfig()
                  javaProperties.forEach { case (k, v) =>
                    logger.info(s"Adding property $k")
                    config.addDataSourceProperty(k.toString, v.toString)
                  }
                  config.setJdbcUrl(url)
                  config.setDriverClassName(driver)
                  config.setMinimumIdle(1)
                  config.setMaximumPoolSize(
                    100
                  ) // dummy value since we are limited by the ForJoinPool size
                  logger.info(s"Creating connection pool for $url")
                  new HikariDataSource(config)
                }
              )
            val connection = pool.getConnection()

            connection
          } else {
            logger.info("Not using connection pooling")
            javaProperties.forEach { case (k, v) =>
              println(s"connecting using property $k=$v")
            }
            DriverManager.getConnection(url, javaProperties)
          }
        //
        if (url.startsWith("jdbc:starlake:")) {
          dataBranch match {
            case Some(branch) if branch.nonEmpty => StarlakeJdbcOps.branchStart(branch, connection)
            case _                               =>
          }
        }
        connection
      }
    }
  }
  val lastExportTableName = "SL_LAST_EXPORT"

  def isExtractCommandHack(url: String) = {
    Set("extract-data", "extract-schema").contains(Main.currentCommand) &&
    !sys.env.contains("SL_API") &&
    url.startsWith("jdbc:duckdb")
  }

  /** Execute a block of code in the context of a newly created connection. We better use here a
    * Connection pool, but since starlake processes are short lived, we do not really need it.
    *
    * @param connectionSettings
    * @param f
    * @param settings
    * @tparam T
    * @return
    */
  private var depth = 0
  private var count = 0
  def withJDBCConnection[T](
    dataBranch: Option[String],
    connectionOptions: Map[String, String],
    existingConnection: Option[Connection] = None
  )(f: Connection => T): T = {
    count = count + 1
    val conn =
      Try {
        existingConnection.getOrElse(
          StarlakeConnectionPool.getConnection(
            dataBranch,
            connectionOptions.removedAll(List("preActions", "postActions"))
          )
        )
      }
    conn match {
      case Failure(exception) =>
        // logger.error(s"count=$count / depth=$depth Error creating connection", exception)
        throw exception
      case Success(connection) =>
        // logger.info(s"count=$count / depth=$depth Created connection $connection")
        depth = depth + 1
        // run preActions
        val preActions = connectionOptions.get("preActions")
        preActions.foreach { actions =>
          actions.split(";").foreach { action =>
            Try {
              val statement = connection.createStatement()
              statement.execute(action)
              statement.close()
            } match {
              case Failure(exception) =>
                logger.error(s"Error running preAction $action", exception)
                throw exception
              case Success(value) =>
            }
          }
        }
        val result = Try {
          f(connection)
        } match {
          case Failure(exception) =>
            logger.error(s"Error running sql", exception)
            Failure(exception)
          case Success(value) =>
            val postActions = connectionOptions.get("postActions")
            postActions.foreach { actions =>
              actions.split(";").foreach { action =>
                Try {
                  val statement = connection.createStatement()
                  statement.execute(action)
                  statement.close()
                } match {
                  case Failure(exception) =>
                    logger.error(s"Error running postAction $action", exception)
                    throw exception
                  case Success(value) =>
                }
              }
            }
            Success(value)
        }

        val url = connectionOptions.get("url").orNull
        if (existingConnection.isEmpty && url != null && !isExtractCommandHack(url)) {
          Try(connection.close()) match {
            case Success(_) =>
              logger.debug(s"Closed connection $url")

            case Failure(exception) =>
              logger.warn(s"Could not close connection to $url", exception)
          }
        }
        depth = depth - 1
        result match {
          case Failure(exception) =>
            throw exception
          case Success(value) => value
        }
    }
  }
  def readOnlyConnection(
    connection: ConnectionInfo
  )(implicit settings: Settings): ConnectionInfo = {

    val options =
      if (connection.isDuckDb()) {
        val duckDbEnableExternalAccess =
          settings.appConfig.duckDbEnableExternalAccess || connection.isMotherDuckDb()
        connection.options
          .updated("duckdb.read_only", "true")
          .updated("access_mode", "READ_ONLY")
          .updated("enable_external_access", duckDbEnableExternalAccess.toString)

      } else {
        connection.options
      }
    connection.copy(options = options)
  }

  def truncateTable(conn: java.sql.Connection, tableName: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.executeUpdate(s"TRUNCATE TABLE $tableName")
    } finally {
      statement.close()
    }
  }

  @throws[Exception]
  def createSchema(conn: Connection, domainName: String): Unit = {
    executeUpdate(schemaCreateSQL(domainName), conn) match {
      case Success(_) =>
      case Failure(e) =>
        logger.error(s"Error creating schema $domainName", e)
        throw e
    }
  }
  @throws[Exception]
  def createDatabase(conn: Connection, dbName: String): Unit = {
    executeUpdate(databaseCreateSQL(dbName), conn) match {
      case Success(_) =>
      case Failure(e) =>
        logger.error(s"Error creating database $dbName", e)
        throw e
    }
  }

  @throws[Exception]
  def schemaCreateSQL(domainName: String): String = {
    s"CREATE SCHEMA IF NOT EXISTS $domainName"
  }

  @throws[Exception]
  def databaseCreateSQL(domainName: String): String = {
    s"CREATE DATABASE IF NOT EXISTS $domainName"
  }

  def buildDropTableSQL(tableName: String): String = {
    s"DROP TABLE IF EXISTS $tableName"
  }
  @throws[Exception]
  def dropTable(conn: Connection, tableName: String): Unit = {
    executeUpdate(buildDropTableSQL(tableName), conn) match {
      case Success(_) =>
      case Failure(e) =>
        logger.error(s"Error creating schema $tableName", e)
        throw e
    }
  }

  def tableExists(conn: Connection, url: String, domainAndTablename: String): Boolean = {
    val dialect = SparkUtils.dialectForUrl(url)
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

  def executeQuery[T](
    stmt: PreparedStatement
  )(apply: ResultSet => T): T = {
    val rs = stmt.executeQuery()
    val result = apply(rs)
    rs.close()
    stmt.close()
    result
  }

  def executeQueryAsMap(
    query: String,
    connection: Connection
  ): List[Map[String, String]] = {
    val resultTable = ListBuffer[Map[String, String]]()
    val statement = connection.createStatement()
    logger.info(s"executing $query")
    try {
      // Establish the connection
      val resultSet = statement.executeQuery(query)

      // Get column names
      val metaData = resultSet.getMetaData
      val columnCount = metaData.getColumnCount
      val columnNames = (1 to columnCount).map(metaData.getColumnName)

      // Process the result set
      while (resultSet.next()) {
        val row = columnNames
          .map(name => name -> Option(resultSet.getObject(name)).map(_.toString).getOrElse("null"))
          .toMap
        resultTable += row
      }
    } finally {
      statement.close()
    }

    resultTable.toList
  }

  def execute(script: String, connection: Connection): Try[Boolean] = {
    val statement = connection.createStatement()
    val result = Try {
      logger.info(s"execute statement: $script")
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

  def executeUpdate(script: String, connection: Connection): Try[Boolean] = {
    val sqlId = java.util.UUID.randomUUID.toString
    val trimmedSQL = script.trim
    val formattedSQL = SQLUtils
      .format(trimmedSQL, JSQLFormatter.OutputFormat.PLAIN)
    logger.info(s"Running JDBC SQL with id $sqlId: $formattedSQL")
    val statement = connection.createStatement()
    val result = Try {
      val count = statement.executeUpdate(trimmedSQL)
      logger.info(s"$count records affected")
      true
    }
    result match {
      case Failure(exception) =>
        logger.error(
          s"Error running JDBC SQL [$trimmedSQL] with id $sqlId: ${exception.getMessage}"
        )
        throw exception
      case Success(value) =>
        logger.info(s"end running JDBC SQL [$trimmedSQL] with id $sqlId with return value $value")
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
    withJDBCConnection(settings.schemaHandler().dataBranch(), connectionOptions) { conn =>
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
    connection: Connection,
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

  private def extractCaseInsensitiveSchemaName(
    connectionSettings: ConnectionInfo,
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

  def extractSchemasAndTableNames(connectionSettings: ConnectionInfo)(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Try[List[(DomainName, List[TableName])]] = {
    extractSchemasAndTableNamesUsingInformationSchema(connectionSettings) match {
      case Success(value) =>
        Success(value)
      case Failure(exception) =>
        logger.warn(
          s"Could not extract schemas and tables using information schema: ${exception.getMessage}"
        )
        extractSchemasAndTableNamesUsingDatabaseMetadata(connectionSettings)
    }
  }

  private def extractSchemasAndTableNamesUsingInformationSchema(
    connectionSettings: ConnectionInfo
  )(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Try[List[(DomainName, List[TableName])]] = Try {
    val result = ListBuffer[(String, String)]()
    ParUtils.runOneInExecutionContext {
      withJDBCConnection(
        // We ignore the branch when reading the information schema
        None, // settings.schemaHandler().dataBranch(),
        readOnlyConnection(connectionSettings).options
      ) { connection =>
        val statement = connection.prepareStatement("""
              |SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
              |WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
              |GROUP BY TABLE_SCHEMA, TABLE_NAME
              |""".stripMargin)
        JdbcDbUtils.executeQuery(statement) { rs =>
          while (rs.next()) {
            val schema = rs.getString(1)
            val table = rs.getString(2)
            logger.info(s"Schema: $schema, Table: $table")
            result.append(schema -> table)
          }
        }
      }
    }(dbExtractEC.executionContext)
    result.groupBy(_._1).toList.map { case (schema, tables) => schema -> tables.map(_._2).toList }
  }

  def extractColumnsUsingInformationSchema(
    connectionSettings: ConnectionInfo,
    tableSchema: String,
    tableName: String
  )(implicit
    settings: Settings
  ): Try[List[(String, SqlColumn)]] = Try {
    val result = ListBuffer[(String, SqlColumn)]()
    withJDBCConnection(
      // We ignore the branch when reading the information schema
      None, // settings.schemaHandler().dataBranch(),
      readOnlyConnection(connectionSettings).options
    ) { connection =>
      val statement = connection.prepareStatement(s"""
                                                     |SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
                                                     |WHERE LOWER(TABLE_SCHEMA) LIKE LOWER('$tableSchema') AND LOWER(TABLE_NAME) LIKE LOWER('$tableName')
                                                     |ORDER BY ORDINAL_POSITION
                                                     |""".stripMargin)
      JdbcDbUtils.executeQuery(statement) { rs =>
        while (rs.next()) {
          val columnName = rs.getString(1)
          val dataType = rs.getString(2)
          val precision = Option(rs.getObject(3)).map(_.toString)
          val scale = Option(rs.getObject(4)).map(_.toString)
          logger.info(
            s"Column: $columnName, Data Type: $dataType, Precision: $precision, Scale: $scale"
          )
          val col = SqlColumn(columnName, dataType, precision, scale)
          result.append(columnName -> col)
        }
      }
    }
    result.toList
  }

  def existsTableUsingInformationSchema(
    connectionSettings: ConnectionInfo,
    tableSchema: String,
    tableName: String
  )(implicit
    settings: Settings
  ): Try[Boolean] = Try {
    withJDBCConnection(
      // We ignore the branch when reading the information schema
      None, // settings.schemaHandler().dataBranch(),
      readOnlyConnection(connectionSettings).options
    ) { connection =>
      val statement = connection.prepareStatement(s"""
                                                     |SELECT COUNT(*) as CNT FROM INFORMATION_SCHEMA.TABLES
                                                     |WHERE LOWER(TABLE_SCHEMA) LIKE LOWER('$tableSchema') AND LOWER(TABLE_NAME) LIKE LOWER('$tableName')
                                                     |""".stripMargin)
      JdbcDbUtils.executeQuery(statement) { rs =>
        rs.next()
      }
    }
  }

  private def extractSchemasAndTableNamesUsingDatabaseMetadata(connectionSettings: ConnectionInfo)(
    implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Try[List[(DomainName, List[TableName])]] = {
    val schemaNames = extractJDBCSchemas(connectionSettings)
    schemaNames.map { schemaNames =>
      val result =
        schemaNames.map { schemaName =>
          val jdbcSchema = JDBCSchema(schema = schemaName, tableTypes = List("TABLE"))
          val tables = extractJDBCTables(
            jdbcSchema,
            connectionSettings,
            skipRemarks = true,
            keepOriginalName = true,
            includeColumns = false
          )
          logger.info(s"Extracted tables for schema $schemaName: ${tables.keys.mkString(", ")}")
          schemaName -> tables.keys.toList.sorted
        }
      result.sortBy(_._1)
    }
  }

  def extractJDBCSchemas(connectionSettings: ConnectionInfo)(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Try[List[String]] = {
    ParUtils.runOneInExecutionContext {
      withJDBCConnection(
        // We ignore the branch when extracting tables
        None, // settings.schemaHandler().dataBranch(),
        readOnlyConnection(connectionSettings).options
      ) { connection =>
        val catalog = connectionSettings.getCatalog()
        val databaseMetaData = connection.getMetaData()
        Using(databaseMetaData.getSchemas(catalog, null)) { resultSet =>
          new Iterator[String] {
            override def hasNext: Boolean = resultSet.next()

            override def next(): String =
              resultSet.getString("TABLE_SCHEM")
          }.toList.filterNot(_.equalsIgnoreCase("INFORMATION_SCHEMA")).distinct.sorted
        }
      }
    }(dbExtractEC.executionContext)
  }

  /* Extract all tables from the database and return Map of tablename -> tableDescription */
  def extractTableNames(
    schemaName: String,
    jdbcSchema: JDBCSchema,
    sqlDefinedTables: List[String],
    tablePredicate: String => Boolean,
    connectionSettings: ConnectionInfo,
    databaseMetaData: DatabaseMetaData,
    skipRemarks: Boolean,
    jdbcEngine: Option[JdbcEngine],
    connection: Connection
  )(implicit settings: Settings): Map[String, Option[String]] = {
    Try {
      val tableTypes =
        if (jdbcSchema.tableTypes.nonEmpty) jdbcSchema.tableTypes.toArray else null
      connectionSettings match {
        case d if d.isMySQLOrMariaDb() =>
          databaseMetaData.getTables(
            schemaName,
            "%",
            "%",
            tableTypes
          )
        case d if d.isDuckDb() =>
          // https://duckdb.org/docs/sql/information_schema.html
          val tableTypesWithBaseTable = jdbcSchema.tableTypes.map { tt =>
            if (tt == "TABLE")
              "BASE TABLE"
            else
              tt

          }.toArray
          val tableTypes =
            if (tableTypesWithBaseTable.nonEmpty) tableTypesWithBaseTable else null
          val resultset =
            databaseMetaData.getTables(
              jdbcSchema.catalog.orNull,
              schemaName,
              "%",
              tableTypes
            )
          resultset
        case _ =>
          val resultset =
            databaseMetaData.getTables(
              jdbcSchema.catalog.orNull,
              schemaName,
              "%",
              tableTypes
            )
          resultset

      }
    }.flatMap { resultSet =>
      Using(resultSet) { resultSet =>
        val tableNamesWithRemarks = new Iterator[(TableName, Option[TableRemarks])] {
          override def hasNext: Boolean = resultSet.next()

          override def next(): (TableName, Option[TableRemarks]) =
            resultSet.getString("TABLE_NAME") -> Option(resultSet.getString("REMARKS"))
        }.toSet ++ sqlDefinedTables.map(_ -> None).toSet
        tableNamesWithRemarks
          .filter { case (tableName, _) => tablePredicate(tableName) }
          .map { case (tableName, metadataRemarks) =>
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
            val remarks = localRemarks.orElse(metadataRemarks)
            tableName -> remarks
          }
          .toMap
      }
    } match {
      case Success(results) => results
      case Failure(exception) =>
        logger.warn(Utils.exceptionAsString(exception))
        logger.warn(
          s"The following schema could not be found $schemaName. All tables within this schema are not ignored."
        )
        Map.empty
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
    connectionSettings: ConnectionInfo,
    skipRemarks: Boolean,
    keepOriginalName: Boolean,
    includeColumns: Boolean
  )(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Map[TableName, ExtractTableAttributes] = {
    val url = connectionSettings.options("url")
    val jdbcServer = url.split(":")(1)
    val jdbcEngine = settings.appConfig.jdbcEngines.get(jdbcServer)
    val jdbcTableMap =
      jdbcSchema.tables
        .map(tblSchema => tblSchema.name.toUpperCase -> tblSchema)
        .toMap
    val uppercaseTableNames = jdbcTableMap.keys.toList
    val schemaAndTableNames = ParUtils.runOneInExecutionContext {
      withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        readOnlyConnection(connectionSettings).options
      ) { connection =>
        val databaseMetaData = connection.getMetaData()
        extractCaseInsensitiveSchemaName(
          connectionSettings,
          databaseMetaData,
          jdbcSchema.schema
        ).map { schemaName =>
          val lowerCasedExcludeTables = jdbcSchema.exclude.map(_.toLowerCase)

          def tablesInScopePredicate(tablesToExtract: List[String] = Nil): TableName => Boolean =
            (tableName: String) => {
              !lowerCasedExcludeTables.contains(
                tableName.toLowerCase
              ) && (tablesToExtract.isEmpty || tablesToExtract.contains(tableName.toUpperCase()))
            }

          val sqlDefinedTables = jdbcSchema.tables.filter(_.sql.isDefined).map(_.name)
          val selectedTables = uppercaseTableNames match {
            case list if list.isEmpty || list.contains("*") =>
              extractTableNames(
                schemaName,
                jdbcSchema,
                sqlDefinedTables,
                tablesInScopePredicate(),
                connectionSettings,
                databaseMetaData,
                skipRemarks,
                jdbcEngine,
                connection
              )
            case list =>
              val extractedTableNames =
                extractTableNames(
                  schemaName,
                  jdbcSchema,
                  sqlDefinedTables,
                  tablesInScopePredicate(list),
                  connectionSettings,
                  databaseMetaData,
                  skipRemarks,
                  jdbcEngine,
                  connection
                )
              val notExtractedTable = list.diff(
                extractedTableNames
                  .map { case (tableName, _) => tableName }
                  .map(_.toUpperCase())
                  .toList
              )
              if (notExtractedTable.nonEmpty) {
                val tablesNotExtractedStr = notExtractedTable.mkString(", ")
                logger.warn(
                  s"The following tables where not extracted for $schemaName.${jdbcSchema.schema} : $tablesNotExtractedStr"
                )
              }
              extractedTableNames
          }
          logger.whenDebugEnabled {
            selectedTables.keys.foreach(table => logger.debug(s"Selected: $table"))
          }
          Utils.printOut(s"Found ${selectedTables.size} tables in $schemaName")
          (schemaName, selectedTables)
        }
      }
    }(dbExtractEC.executionContext)
    val res =
      if (includeColumns) {
        val res =
          schemaAndTableNames.flatMap { case (schemaName, selectedTableNames) =>
            // Extract the Starlake Schema
            Using
              .Manager { use =>
                ParUtils
                  .runInExecutionContext(selectedTableNames.toList) {
                    case (tableName, tableRemarks) =>
                      ExtractUtils.timeIt(s"Table's schema extraction of $schemaName.$tableName") {
                        Utils.printOut(
                          s"Extracting table '$schemaName.$tableName'"
                        )
                        withJDBCConnection(
                          settings.schemaHandler().dataBranch(),
                          readOnlyConnection(connectionSettings).options
                        ) { tableExtractConnection =>
                          val jdbcColumnMetadata: JdbcColumnMetadata =
                            jdbcSchema.tables
                              .find(_.name.equalsIgnoreCase(tableName))
                              .flatMap(_.sql)
                              .map { sql =>
                                // extract schema from sql metadata
                                val statement = tableExtractConnection.createStatement()
                                statement.setMaxRows(1)
                                val resultSet = use(statement.executeQuery(sql))
                                new ResultSetColumnMetadata(
                                  resultSet.getMetaData,
                                  jdbcSchema,
                                  tableName,
                                  keepOriginalName,
                                  skipRemarks,
                                  jdbcEngine
                                )
                              }
                              .getOrElse(
                                new JdbcColumnDatabaseMetadata(
                                  connectionSettings,
                                  tableExtractConnection.getMetaData,
                                  jdbcSchema,
                                  schemaName,
                                  tableName,
                                  keepOriginalName,
                                  skipRemarks,
                                  jdbcEngine
                                )
                              )
                          val primaryKeys = jdbcColumnMetadata.primaryKeys
                          val foreignKeys: Map[TableName, TableName] =
                            jdbcColumnMetadata.foreignKeys
                          val columns: List[TableAttribute] = jdbcColumnMetadata.columns
                          logger.whenDebugEnabled {
                            columns
                              .foreach(column => logger.debug(s"column: $tableName.${column.name}"))
                          }
                          val jdbcCurrentTable = jdbcTableMap
                            .get(tableName.toUpperCase)
                          // Limit to the columns specified by the user if any
                          val currentTableRequestedColumns: Map[ColumnName, Option[ColumnName]] =
                            jdbcCurrentTable
                              .map(
                                _.columns.map(c =>
                                  (if (keepOriginalName) c.name.toUpperCase.trim
                                   else c.rename.getOrElse(c.name).toUpperCase.trim) -> c.rename
                                )
                              )
                              .getOrElse(Map.empty)
                              .toMap
                          val currentFilter = jdbcCurrentTable.flatMap(_.filter)
                          val selectedColumns: List[TableAttribute] =
                            columns
                              .filter(col =>
                                currentTableRequestedColumns.isEmpty || currentTableRequestedColumns
                                  .contains("*") || currentTableRequestedColumns
                                  .contains(col.name.toUpperCase())
                              )
                              .map(c =>
                                c.copy(
                                  foreignKey = foreignKeys.get(c.name.toUpperCase)
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
                          tableName -> ExtractTableAttributes(
                            tableRemarks,
                            selectedColumns,
                            primaryKeys,
                            currentFilter
                          )
                        }
                      }
                  }(dbExtractEC.executionContext)
                  .toMap
              }
          }
        res
      } else {
        schemaAndTableNames.map { case (schemaName, selectedTableNames) =>
          selectedTableNames.toList.map { case (tableName, tableRemarks) =>
            Utils.printOut(
              s"Extracting table '$schemaName.$tableName'"
            )
            tableName -> ExtractTableAttributes(
              tableRemarks,
              Nil,
              Nil,
              None
            )
          }.toMap

        }
      }
    res match {
      case Failure(exception) =>
        Utils.logException(logger, exception)
        Map.empty
      case Success(value) => value
    }
  }

  def formatRemarksSQL(
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
    domainTemplate: Option[DomainInfo],
    selectedTablesAndColumns: Map[String, ExtractTableAttributes]
  ): DomainInfo = {
    def isNumeric(sparkType: String): Boolean = {
      sparkType match {
        case "double" | "decimal" | "long" => true
        case _                             => false
      }
    }

    val trimTemplate =
      domainTemplate.flatMap(_.tables.headOption.flatMap(_.attributes.head.trim))

    val starlakeSchema = selectedTablesAndColumns.map { case (tableName, tableAttrs) =>
      val sanitizedTableName = StringUtils.replaceNonAlphanumericWithUnderscore(tableName)
      SchemaInfo(
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
        comment = tableAttrs.tableRemarks,
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

    val normalizedDomainName = StringUtils.replaceNonAlphanumericWithUnderscore(jdbcSchema.schema)
    val rename = domainTemplate
      .flatMap(_.rename)
      .map { name =>
        DatasetArea.substituteDomainAndSchema(jdbcSchema.schema, "", name)
      }
      .orElse(
        if (normalizedDomainName != jdbcSchema.schema) Some(normalizedDomainName) else None
      )
    val ack = domainTemplate.flatMap(_.resolveAck())

    DomainInfo(
      database = database,
      name = jdbcSchema.sanitizeName match {
        case Some(true) => StringUtils.replaceNonAlphanumericWithUnderscore(jdbcSchema.schema)
        case _          => jdbcSchema.schema
      },
      rename = rename,
      metadata = domainTemplate
        .flatMap(_.metadata)
        .map(_.copy(directory = incomingDir, ack = ack)),
      tables = starlakeSchema.toList,
      comment = None
    )
  }

  def jdbcOptions(
    jdbcOptions: Map[String, String],
    sparkFormat: String,
    accessToken: Option[String]
  ): CaseInsensitiveMap[TableName] = {
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
    val optionsWithAccessToken = accessToken match {
      case Some(token) =>
        options + ("sl_access_token" -> token)
      case None => options
    }
    CaseInsensitiveMap[String](optionsWithAccessToken)
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
  sealed trait BoundaryDef
  case class Boundary(lower: BoundType, upper: BoundType) extends BoundaryDef
  case object Unbounded extends BoundaryDef

  sealed trait BoundariesDef

  case class Bounds(
    typ: PrimitiveType,
    count: Long,
    max: Any,
    partitions: List[Boundary]
  ) extends BoundariesDef

  case object NoBound extends BoundariesDef

  private val MIN_TS = Timestamp.valueOf("1970-01-01 00:00:00")
  private val MIN_DATE = java.sql.Date.valueOf("1970-01-01")
  private val MIN_DECIMAL = java.math.BigDecimal.ZERO

  def getBoundaries(
    conn: Connection,
    auditConnection: Connection,
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
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
          JdbcDbUtils.executeQuery(statement) { rs =>
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
          JdbcDbUtils.executeQuery(statement) { rs =>
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
          JdbcDbUtils.executeQuery(statement) { rs =>
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
          JdbcDbUtils.executeQuery(statement) { rs =>
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
              JdbcDbUtils.executeQuery(statement) { rs =>
                rs.next()
                val count = rs.getLong(1)
                // The algorithm to fetch data doesn't support null values so putting 0 as default value is OK.
                val min: Long = Option(rs.getLong(2)).getOrElse(0)
                val max: Long = Option(rs.getLong(3)).getOrElse(0)
                (count, min, max)
              }
            case java.sql.Types.INTEGER =>
              statement.setInt(1, Int.MinValue)
              JdbcDbUtils.executeQuery(statement) { rs =>
                rs.next()
                val count = rs.getLong(1)
                // The algorithm to fetch data doesn't support null values so putting 0 as default value is OK.
                val min: Long = Option(rs.getInt(2)).getOrElse(0).toLong
                val max: Long = Option(rs.getInt(3)).getOrElse(0).toLong
                (count, min, max)
              }
            case java.sql.Types.SMALLINT =>
              statement.setShort(1, Short.MinValue)
              JdbcDbUtils.executeQuery(statement) { rs =>
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
                PrimitiveType.long,
                count,
                0,
                List.empty
              )
            case _ =>
              val partitions =
                (min to max).map(p => Boundary(InclusiveBound(p), ExclusiveBound(p + 1))).toList
              Bounds(
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

  /** @return
    *   internal boundaries from custom sql query if given, otherwise fetch from defined table
    *   directly.
    */
  private def internalBoundaries[T](
    conn: Connection,
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    hashFunc: Option[String]
  )(apply: PreparedStatement => T): T = {
    val extraCondition = tableExtractDataConfig.filterOpt.map(w => s"and $w").getOrElse("")
    val quotedColumn = extractConfig.data.quoteIdentifier(tableExtractDataConfig.partitionColumn)
    val columnToDistribute = hashFunc.getOrElse(quotedColumn)
    val dataSource = tableExtractDataConfig.sql
      .map("(" + _ + ") sl_data_source")
      .getOrElse(s"${extractConfig.data.quoteIdentifier(
          tableExtractDataConfig.domain
        )}.${extractConfig.data.quoteIdentifier(tableExtractDataConfig.table)}")
    val SQL_BOUNDARIES_VALUES =
      s"""select count($quotedColumn) as count_value, min($columnToDistribute) as min_value, max($columnToDistribute) as max_value
         |from $dataSource
         |where $columnToDistribute > ? $extraCondition""".stripMargin
    val preparedStatement = conn.prepareStatement(SQL_BOUNDARIES_VALUES)
    apply(preparedStatement)
  }

  def getMaxLongFromSuccessfulExport(
    conn: Connection,
    extractConfig: ExtractJdbcDataConfig,
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
    conn: Connection,
    extractConfig: ExtractJdbcDataConfig,
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
    conn: Connection,
    extractConfig: ExtractJdbcDataConfig,
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
    conn: Connection,
    extractConfig: ExtractJdbcDataConfig,
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
    conn: Connection,
    extractConfig: ExtractJdbcDataConfig,
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
    conn: Connection,
    row: DeltaRow,
    partitionColumnType: Option[PrimitiveType],
    connectionSettings: ConnectionInfo,
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
