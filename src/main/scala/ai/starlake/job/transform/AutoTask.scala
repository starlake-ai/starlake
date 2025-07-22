/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.common.TaskSQLStatements
import ai.starlake.job.ingest.{AuditLog, Step}
import ai.starlake.job.metrics.{ExpectationJob, JdbcExpectationAssertionHandler}
import ai.starlake.job.sink.bigquery.BigQueryJobBase
import ai.starlake.job.strategies.TransformStrategiesBuilder
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.sql.SQLUtils
import ai.starlake.transpiler.JSQLTranspiler
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.*
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

/** Execute the SQL Task and store it in parquet/orc/.... If Hive support is enabled, also store it
  * as a Hive Table. If analyze support is active, also compute basic statistics for twhe dataset.
  *
  * @param name
  *   : Job Name as defined in the YML job description file
  * @param interactive
  *   : If the task is interactive, it will not be materialized. Just the select statement will be
  *   executed
  * @param defaultArea
  *   : Where the resulting dataset is stored by default if not specified in the task
  * @param taskDesc
  *   : Task to run
  * @param commandParameters
  *   : Sql Parameters to pass to SQL statements
  */
abstract class AutoTask(
  val appId: Option[String],
  val taskDesc: AutoTaskInfo,
  val commandParameters: Map[String, String],
  val interactive: Option[String],
  val test: Boolean,
  val logExecution: Boolean,
  val truncate: Boolean = false,
  val resultPageSize: Int,
  val resultPageNumber: Int,
  val accessToken: Option[String],
  conn: Option[java.sql.Connection]
)(implicit val settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends SparkJob {

  def createAuditTable(): Boolean

  /** Build the SQL statements to create or alter the table schema in the target database.
    * @param incomingSchema
    * @param tableName
    * @return
    *   A list of SQL statements to create or alter the table schema and a boolean indicating
    *   whether the table existed before the operation.
    *
    * This method is expected to be implemented by subclasses to provide the specific SQL statements
    * needed for the target database engine. For BigQuery and Spark, no need to implement it since
    * these are schema on write databases
    */
  def buildTableSchemaSQL(
    incomingSchema: StructType,
    tableName: String
  ): (List[String], Boolean) = (Nil, true)

  lazy val fullDomainName = taskDesc.database match {
    case Some(db) => s"$db.${taskDesc.domain}"
    case None     => taskDesc.domain
  }

  def aclSQL(): List[String] = {
    val sinkEngine = sinkConnection.getJdbcEngineName()
    taskDesc.acl.flatMap { ace =>
      /*
        https://docs.snowflake.com/en/sql-reference/sql/grant-privilege
        https://hevodata.com/learn/snowflake-grant-role-to-user/
       */
      ace.asSql(fullTableName, sinkEngine)
    }
  }

  override def applicationId(): String = appId.getOrElse(super.applicationId())

  def attDdl(): Map[String, Map[String, String]] =
    schemaHandler
      .domains()
      .find(_.finalName == taskDesc.domain)
      .flatMap(_.tables.find(_.finalName == taskDesc.table))
      .map(schemaHandler.getDdlMapping)
      .getOrElse(Map.empty)

  val sparkSinkFormat =
    taskDesc.sink.flatMap(_.format).getOrElse(settings.appConfig.defaultWriteFormat)

  val sinkConfig = taskDesc.getSinkConfig()

  val sinkOptions =
    if (sinkConnection.isDuckDb()) {
      val duckDbEnableExternalAccess =
        settings.appConfig.duckDbEnableExternalAccess || sinkConnection.isMotherDuckDb()
      sinkConnection.options.updated(
        "enable_external_access",
        duckDbEnableExternalAccess.toString
      )
    } else {
      sinkConnection.options
    }
  def fullTableName: String

  def run(): Try[JobResult]

  override def name: String = taskDesc.name

  protected lazy val sinkConnectionRef: String =
    sinkConfig.connectionRef.getOrElse(settings.appConfig.connectionRef)

  protected lazy val sinkConnection: Settings.Connection =
    settings.appConfig.connections(sinkConnectionRef).withAccessToken(accessToken)

  protected def strategy: WriteStrategy = taskDesc.getStrategy()

  protected def isMerge(sql: String): Boolean = {
    sql.toLowerCase().contains("merge into")
  }

  def tableExists: Boolean

  protected lazy val allVars =
    schemaHandler.activeEnvVars() ++ commandParameters // ++ Map("merge" -> tableExists)
  protected lazy val preSql = {
    val testMacros =
      if (this.test) {
        List("LOAD SPATIAL", "LOAD JSON") ++ JSQLTranspiler.getMacroArray.toList
      } else
        Nil
    testMacros ++ parseJinja(taskDesc.presql, allVars).filter(_.trim.nonEmpty)
  }
  protected lazy val postSql = parseJinja(taskDesc.postsql, allVars).filter(_.trim.nonEmpty)

  lazy val jdbcSinkEngineName = this.sinkConnection.getJdbcEngineName()
  lazy val jdbcSinkEngine = settings.appConfig.jdbcEngines(jdbcSinkEngineName.toString)

  def buildRLSQueries(): List[String]

  def buildConnection(): Map[String, String] = {
    sinkConnection.asMap()
  }
  def buildAllSQLQueries(
    sql: Option[String],
    tableExistsForcedValue: Option[Boolean] = None,
    forceNative: Boolean = false
  ): String = {
    val inputSQL = sql.getOrElse(taskDesc.getSql())
    val runConnection =
      if (forceNative) {
        this.taskDesc.getRunConnection().copy(sparkFormat = None)
      } else {
        this.taskDesc.getRunConnection()
      }

    if (interactive.isEmpty) {
      if (taskDesc.parseSQL.getOrElse(true)) {
        val sqlWithParametersTranspiledIfInTest =
          schemaHandler.transpileAndSubstitute(
            inputSQL,
            runConnection,
            allVars,
            this.test
          )

        val tableComponents = TransformStrategiesBuilder.TableComponents(
          taskDesc.database.getOrElse(""), // Convert it to "" for jinjava to work
          taskDesc.domain,
          taskDesc.table,
          SQLUtils.extractColumnNames(sqlWithParametersTranspiledIfInTest)
        )
        val jdbcRunEngineName: Engine = runConnection.getJdbcEngineName()
        val jdbcRunEngine = settings.appConfig.jdbcEngines(jdbcRunEngineName.toString)

        val tblExists =
          tableExistsForcedValue.getOrElse(
            tableExists
          ) // If tableExistsForcedValue is defined, use it, otherwise use tableExists
        val mainSql = TransformStrategiesBuilder().buildTransform(
          strategy,
          sqlWithParametersTranspiledIfInTest,
          tableComponents,
          tblExists,
          truncate = truncate,
          materializedView = resolveMaterializedView(),
          jdbcRunEngine,
          sinkConfig
        )
        if (settings.appConfig.syncSqlWithYaml && taskDesc._auditTableName.isEmpty) {
          val list = schemaHandler.syncPreviewSqlWithYaml(taskDesc.getName(), None, None)
          schemaHandler.syncApplySqlWithYaml(taskDesc.getName(), list)
        }
        if (settings.appConfig.syncYamlWithDb && taskDesc._auditTableName.isEmpty) {
          logger.info(s"Main SQL: $mainSql")
          logger.info("Identifying new / altered columns for " + fullTableName)
          val columnStatements =
            if (tableExists) {
              val (columnStatements, _) =
                buildTableSchemaSQL(
                  this.taskDesc.sparkSchema(schemaHandler),
                  this.fullTableName
                )
              logger.info(s"${columnStatements.length} Schema change(s) to apply:")
              columnStatements.foreach { stmt =>
                logger.info(s" - $stmt")
              }
              columnStatements
            } else {
              logger.info("No schema changes to apply for " + fullTableName)
              Nil
            }

          columnStatements.mkString("", ";\n", ";\n") + mainSql
        } else {
          mainSql
        }

      } else {
        val mainSql = schemaHandler.substituteRefTaskMainSQL(
          inputSQL,
          taskDesc.getRunConnection(),
          allVars
        )
        mainSql
      }
    } else {
      val sqlWithParametersTranspiledIfInTest =
        schemaHandler.transpileAndSubstitute(
          inputSQL,
          runConnection,
          allVars,
          this.test
        )
      sqlWithParametersTranspiledIfInTest
    }
  }

  private def parseJinja(sql: String, vars: Map[String, Any]): String = parseJinja(
    List(sql),
    vars
  ).head

  /** All variables defined in the active profile are passed as string parameters to the Jinja
    * parser.
    *
    * @param sqls
    * @return
    */
  protected def parseJinja(
    sqls: List[String],
    vars: Map[String, Any],
    failOnUnknownTokens: Boolean = false
  ): List[String] = {
    val result = Utils
      .parseJinja(
        sqls,
        allVars ++ vars
      )
    logger.debug(s"Parse Jinja result: $result")
    result
  }

  def auditTableCreateSQL(): List[String] = {
    // Table not found and it is an table in the audit schema defined in the reference-connections.conf file  Try to create it.
    logger.info(s"Table ${taskDesc.table} not found in ${taskDesc.domain}")

    val entry = taskDesc._auditTableName.getOrElse(
      throw new Exception(
        s"audit table for output ${taskDesc.table} is not defined in engine $jdbcSinkEngineName"
      )
    )
    val scriptTemplate = jdbcSinkEngine.tables(entry).createSql

    val script = scriptTemplate.richFormat(
      Map("table" -> fullTableName, "writeFormat" -> settings.appConfig.defaultWriteFormat),
      Map.empty
    )
    List(JdbcDbUtils.schemaCreateSQL(fullDomainName), script)
  }

  private def auditLog(
    start: Timestamp,
    end: Timestamp,
    jobResultCount: Long,
    success: Boolean,
    message: String,
    test: Boolean
  ): Option[AuditLog] = {
    if (taskDesc._auditTableName.isEmpty) { // avoid recursion when logging audit
      val log = AuditLog(
        applicationId(),
        Some(this.name),
        this.taskDesc.domain,
        this.taskDesc.table,
        success,
        jobResultCount,
        -1,
        -1,
        start,
        end.getTime - start.getTime,
        message,
        Step.TRANSFORM.toString,
        taskDesc.getDatabase(),
        settings.appConfig.tenant,
        test
      )
      Some(log)
    } else {
      None
    }
  }

  def logAuditSuccess(
    start: Timestamp,
    end: Timestamp,
    jobResultCount: Long,
    test: Boolean
  ): Unit = {
    val log = auditLog(start, end, jobResultCount, success = true, "success", test)
    log.foreach(al => AuditLog.sink(List(al), accessToken))
  }

  def logAuditFailure(start: Timestamp, end: Timestamp, e: Throwable, test: Boolean): Unit = {
    val log = auditLog(start, end, -1, success = false, Utils.exceptionAsString(e), test)
    log.foreach(al => AuditLog.sink(List(al), accessToken))
  }

  def dependencies(streams: CaseInsensitiveMap[String]): List[String] = {
    val result = SQLUtils.extractTableNamesUsingRegEx(parseJinja(taskDesc.getSql(), Map.empty))
    val withStreamsResolved = result.map { table =>
      if (streams.contains(table)) {
        streams(table)
      } else {
        table
      }
    }
    logger.info(
      s"$name has ${withStreamsResolved.length} dependencies: ${withStreamsResolved.mkString(",")}"
    )
    withStreamsResolved
  }

  val (createDisposition, writeDisposition) =
    Utils.getDBDisposition(
      taskDesc.getWriteMode()
    )

  def resolveMaterializedView(): Materialization = {
    taskDesc.sink
      .flatMap(_.materializedView)
      .getOrElse(Materialization.TABLE)
  }

  private def buildAddSCD2ColumnsSqls(engineName: Engine): List[String] = {
    this.taskDesc.writeStrategy match {
      case Some(strategyOptions) if strategyOptions.getEffectiveType() == WriteStrategyType.SCD2 =>
        val startTsCol = strategyOptions.startTs.getOrElse(settings.appConfig.scd2StartTimestamp)
        val endTsCol = strategyOptions.endTs.getOrElse(settings.appConfig.scd2EndTimestamp)
        val scd2Columns = List(startTsCol, endTsCol)
        val alterTableSqls = scd2Columns.map { column =>
          if (engineName.toString.toLowerCase() == "redshift")
            s"ALTER TABLE $fullTableName ADD COLUMN $column TIMESTAMP"
          else
            s"ALTER TABLE $fullTableName ADD COLUMN IF NOT EXISTS $column TIMESTAMP NULL"
        }
        alterTableSqls
      case _ =>
        List.empty
    }
  }

  def expectationStatements(): List[ExpectationSQL] = {
    if (settings.appConfig.expectations.active) {
      // TODO Implement Expectations
      new ExpectationJob(
        Option(applicationId()),
        taskDesc.database,
        taskDesc.domain,
        taskDesc.table,
        taskDesc.expectations,
        storageHandler,
        schemaHandler,
        new JdbcExpectationAssertionHandler(sinkOptions)
      ).buildStatementsList() match {
        case Success(expectations) =>
          expectations
        case Failure(e) =>
          throw e
      }
    } else {
      List.empty
    }
  }

  def auditStatements(): Option[TaskSQLStatements] = {
    if (settings.appConfig.audit.active.getOrElse(true)) {
      val auditStatements =
        auditLog(
          new Timestamp(System.currentTimeMillis()),
          new Timestamp(System.currentTimeMillis()),
          0,
          success = true,
          "success",
          test
        ).flatMap(al => AuditLog.buildListOfSQLStatements(List(al), accessToken))
      auditStatements
    } else {
      None
    }
  }

  def buildListOfSQLStatements(): TaskSQLStatements = {
    val createSchemaAndTableSql =
      if (settings.appConfig.createSchemaIfNotExists) {
        // Creating a schema requires its own connection if called before a Spark save
        if (taskDesc._auditTableName.isDefined)
          this.auditTableCreateSQL()
        else
          List.empty
      } else {
        List.empty
      }

    val mainSqlIfExists = buildAllSQLQueries(None, Some(true)).splitSql()
    val mainSqlIfNotExists = buildAllSQLQueries(None, Some(false)).splitSql()

    val connectionPreActions =
      sinkOptions.get("preActions").map(_.split(';')).getOrElse(Array.empty).toList

    val parsedPreActions =
      Utils
        .parseJinja(
          jdbcSinkEngine.preActions.getOrElse(""),
          Map("schema" -> taskDesc.domain)
        )
        .splitSql(";")
    val preSqls = preSql
    val postSqls = postSql

    val addSCD2ColumnsSqls =
      buildAddSCD2ColumnsSqls(sinkConnection.getJdbcEngineName())

    TaskSQLStatements(
      taskDesc.name,
      taskDesc.domain,
      taskDesc.table,
      createSchemaAndTableSql.map(_.pyFormat()),
      (connectionPreActions ++ parsedPreActions).map(_.pyFormat()),
      preSqls.map(_.pyFormat()),
      mainSqlIfExists.map(_.pyFormat()),
      mainSqlIfNotExists.map(_.pyFormat()),
      postSqls.map(_.pyFormat()),
      addSCD2ColumnsSqls.map(_.pyFormat()),
      taskDesc.getSinkConnectionType()
    )
  }

  protected def limitQuery(sql: String, pageSize: Int, pageNumber: Int): String = {
    val limit =
      if (pageSize > settings.appConfig.maxInteractiveRecords)
        settings.appConfig.maxInteractiveRecords
      else
        pageSize
    val trimmedSql = SQLUtils.stripComments(sql)
    val upperCaseSQL = trimmedSql.toUpperCase().replace("\n", " ")
    if (
      upperCaseSQL.indexOf(" LIMIT ") == -1 &&
      (upperCaseSQL.startsWith("SELECT ") || upperCaseSQL.startsWith("WITH "))
    ) {
      if (trimmedSql.endsWith(";")) {
        val noDelimiterSql = trimmedSql.dropRight(1)
        s"$noDelimiterSql LIMIT $limit OFFSET ${pageSize * (pageNumber - 1)}"
      } else
        s"$sql LIMIT $limit OFFSET ${pageSize * (pageNumber - 1)}"
    } else
      sql
  }
}

object AutoTask extends StrictLogging {

  def minimal(
    domainName: String,
    tableName: String,
    connectionRef: String,
    accessToken: Option[String] = None,
    _auditTableName: Option[String] = None
  )(implicit
    settings: Settings
  ): AutoTask = {
    val desc =
      AutoTaskInfo(
        "__IGNORE__",
        sql = None,
        database = None,
        domain = domainName,
        table = tableName,
        connectionRef = Some(connectionRef),
        _auditTableName = _auditTableName
      )
    AutoTask
      .task(
        appId = None,
        taskDesc = desc,
        configOptions = Map.empty,
        interactive = None,
        accessToken = accessToken,
        test = false,
        truncate = false,
        logExecution = false,
        engine = settings.appConfig.getConnection(connectionRef).getEngine(),
        resultPageSize = 1000,
        resultPageNumber = 1,
        dryRun = false
      )(settings, settings.storageHandler(), settings.schemaHandler())
  }

  /** Used for linegae only
    */
  def unauthenticatedTasks(reload: Boolean)(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): List[AutoTask] = {
    schemaHandler
      .tasks(reload)
      .map(
        task(
          None,
          _,
          Map.empty,
          None,
          engine = Engine.SPARK,
          truncate = false,
          test = false,
          logExecution = true,
          resultPageSize = 200,
          resultPageNumber = 1,
          dryRun = false
        )
      )
  }

  def executeUpdate(sql: String, connectionRef: String, accessToken: Option[String])(implicit
    settings: Settings
  ): Try[Boolean] = {
    val connection = settings.appConfig
      .connection(connectionRef)
      .getOrElse(throw new Exception(s"Connection not found $connectionRef"))
    val engine = connection.getEngine()
    engine match {
      case Engine.BQ =>
        BigQueryJobBase.executeUpdate(sql, connectionRef, accessToken)
      case Engine.JDBC =>
        JdbcAutoTask.executeUpdate(sql, connectionRef, accessToken)
      case Engine.SPARK =>
        SparkAutoTask.executeUpdate(sql, connectionRef /* ignored */, accessToken /* ignored */ )
      case _ =>
        Failure(throw new Exception(s"Unsupported engine $engine"))
    }
  }

  def task(
    appId: Option[String],
    taskDesc: AutoTaskInfo,
    configOptions: Map[String, String],
    interactive: Option[String],
    truncate: Boolean,
    test: Boolean,
    engine: Engine,
    logExecution: Boolean,
    accessToken: Option[String] = None,
    resultPageSize: Int,
    resultPageNumber: Int,
    dryRun: Boolean
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): AutoTask = {
    val sinkConfig = taskDesc.getSinkConfig()
    val runConnectionRef = taskDesc.getRunConnectionRef()
    engine match {
      case Engine.BQ if sinkConfig.isInstanceOf[BigQuerySink] || interactive.isDefined =>
        new BigQueryAutoTask(
          appId,
          taskDesc,
          configOptions,
          interactive,
          truncate = truncate,
          test = test,
          logExecution = logExecution,
          accessToken = accessToken,
          resultPageSize = resultPageSize,
          resultPageNumber = resultPageNumber,
          dryRun = dryRun
        )
      case Engine.JDBC
          if sinkConfig
            .isInstanceOf[JdbcSink] && sinkConfig
            .getConnectionRef() == runConnectionRef || interactive.isDefined =>
        new JdbcAutoTask(
          appId,
          taskDesc,
          configOptions,
          interactive,
          truncate = truncate,
          test = test,
          logExecution = logExecution,
          accessToken = accessToken,
          resultPageSize = resultPageSize,
          resultPageNumber = resultPageNumber,
          conn = None
        )
      case _ =>
        sinkConfig match {
          case fs: FsSink if fs.isExport() && interactive.isEmpty =>
            logger.info("Exporting to the filesystem")
            new SparkExportTask(
              appId = appId,
              taskDesc = taskDesc,
              commandParameters = configOptions,
              interactive = interactive,
              truncate = truncate,
              test = test,
              accessToken = accessToken,
              resultPageSize = resultPageSize,
              logExecution = logExecution,
              resultPageNumber = resultPageNumber
            )

          case _ =>
            new SparkAutoTask(
              appId = appId,
              taskDesc = taskDesc,
              commandParameters = configOptions,
              interactive = interactive,
              truncate = truncate,
              test = test,
              accessToken = accessToken,
              resultPageSize = resultPageSize,
              resultPageNumber = resultPageNumber,
              logExecution = logExecution
            )
        }
    }
  }
  def executeSelect(
    domain: String,
    table: String,
    sql: String,
    summarizeOnly: Boolean,
    connectionName: String,
    accessToken: Option[String],
    test: Boolean,
    parseSQL: Boolean,
    pageSize: Int,
    pageNumber: Int
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[List[Map[String, Any]]] = {
    val connection =
      Try(
        settings.appConfig
          .connection(connectionName)
          .getOrElse(throw new Exception(s"Connection not found $connectionName"))
          .withAccessToken(accessToken)
      )
    connection match {
      case Success(conn) =>
        executeSelect(
          domain,
          table,
          sql,
          summarizeOnly,
          JdbcDbUtils.readOnlyConnection(conn),
          accessToken,
          Some(connectionName),
          test,
          parseSQL,
          pageSize,
          pageNumber
        )
      case Failure(e) =>
        Failure(e)
    }

  }

  def executeSelect(
    domain: String,
    table: String,
    sql: String,
    summarizeOnly: Boolean,
    connection: Settings.Connection,
    accessToken: Option[String],
    connectionName: Option[String],
    test: Boolean,
    parseSQL: Boolean,
    pageSize: Int,
    pageNumber: Int
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[List[Map[String, Any]]] = Try {
    val quote =
      settings.appConfig.jdbcEngines
        .get(connection.getJdbcEngineName().toString)
        .map(_.quote)
        .getOrElse("")

    val finalSql =
      if (summarizeOnly)
        if (connection.isDuckDb())
          s"SUMMARIZE $quote$domain$quote.$quote$table$quote"
        else
          s"DESCRIBE TABLE $quote$domain$quote.$quote$table$quote"
      else
        sql

    val autoTaskDesc = AutoTaskInfo(
      name = s"$domain.$table",
      sql = Some(finalSql),
      domain = domain,
      table = table,
      database = None,
      connectionRef = connectionName,
      parseSQL = Some(parseSQL)
    )
    val engine =
      connection.`type` match {
        case ConnectionType.BQ   => Engine.BQ
        case ConnectionType.JDBC => Engine.JDBC
        case ConnectionType.FS =>
          Engine.SPARK
        case _ =>
          throw new IllegalArgumentException(
            s"Unsupported connection type: ${connection.`type`}"
          )
      }
    val t = task(
      None,
      autoTaskDesc,
      Map.empty,
      Some("json-array"),
      truncate = false,
      test = test,
      engine = engine,
      logExecution = false,
      accessToken = accessToken,
      resultPageSize = pageSize,
      resultPageNumber = pageNumber,
      dryRun = false
    )
    t.run() match {
      case Success(jobResult) =>
        jobResult.asMap()
      case Failure(e) =>
        e.printStackTrace()
        throw e
    }
  }

}
