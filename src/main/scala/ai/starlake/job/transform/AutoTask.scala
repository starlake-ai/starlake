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
import ai.starlake.job.ingest.{AuditLog, Step}
import ai.starlake.job.strategies.StrategiesBuilder
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.transpiler.JSQLTranspiler
import ai.starlake.utils._
import com.typesafe.scalalogging.StrictLogging

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

/** Execute the SQL Task and store it in parquet/orc/.... If Hive support is enabled, also store it
  * as a Hive Table. If analyze support is active, also compute basic statistics for twhe dataset.
  *
  * @param name
  *   : Job Name as defined in the YML job description file
  * @param defaultArea
  *   : Where the resulting dataset is stored by default if not specified in the task
  * @param taskDesc
  *   : Task to run
  * @param commandParameters
  *   : Sql Parameters to pass to SQL statements
  */
abstract class AutoTask(
  val appId: Option[String],
  val taskDesc: AutoTaskDesc,
  val commandParameters: Map[String, String],
  val interactive: Option[String],
  val test: Boolean,
  val logExecution: Boolean,
  val truncate: Boolean = false,
  val resultPageSize: Int = 1
)(implicit val settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends SparkJob {

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

  def fullTableName: String

  def run(): Try[JobResult]

  override def name: String = taskDesc.name

  protected lazy val sinkConnectionRef: String =
    sinkConfig.connectionRef.getOrElse(settings.appConfig.connectionRef)

  protected lazy val sinkConnection: Settings.Connection =
    settings.appConfig.connections(sinkConnectionRef)

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
        JSQLTranspiler.getMacroArray.toList
      } else
        Nil
    testMacros ++ parseJinja(taskDesc.presql, allVars).filter(_.trim.nonEmpty)
  }
  protected lazy val postSql = parseJinja(taskDesc.postsql, allVars).filter(_.trim.nonEmpty)

  lazy val jdbcSinkEngineName = this.sinkConnection.getJdbcEngineName()
  lazy val jdbcSinkEngine = settings.appConfig.jdbcEngines(jdbcSinkEngineName.toString)

  def buildAllSQLQueries(sql: Option[String]): String = {
    val inputSQL = sql.getOrElse(taskDesc.getSql())
    val runConnection = this.taskDesc.getRunConnection()
    if (interactive.isEmpty) {
      if (taskDesc.parseSQL.getOrElse(true)) {
        val sqlWithParametersTranspiledIfInTest =
          schemaHandler.transpileAndSubstitute(
            inputSQL,
            runConnection,
            commandParameters,
            this.test
          )

        val tableComponents = StrategiesBuilder.TableComponents(
          taskDesc.database.getOrElse(""), // Convert it to "" for jinjava to work
          taskDesc.domain,
          taskDesc.table,
          SQLUtils.extractColumnNames(sqlWithParametersTranspiledIfInTest)
        )
        val jdbcRunEngineName: Engine = runConnection.getJdbcEngineName()

        val jdbcRunEngine = settings.appConfig.jdbcEngines(jdbcRunEngineName.toString)

        val mainSql = StrategiesBuilder().run(
          strategy,
          sqlWithParametersTranspiledIfInTest,
          tableComponents,
          tableExists,
          truncate = truncate,
          materializedView = resolveMaterializedView(),
          jdbcRunEngine,
          sinkConfig
        )
        mainSql
      } else {
        val mainSql = schemaHandler.substituteRefTaskMainSQL(
          inputSQL,
          taskDesc.getRunConnection(),
          commandParameters
        )
        mainSql
      }
    } else {
      val sqlWithParametersTranspiledIfInTest =
        schemaHandler.transpileAndSubstitute(
          inputSQL,
          runConnection,
          commandParameters,
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
        schemaHandler.activeEnvVars() ++ commandParameters ++ vars
      )
    logger.debug(s"Parse Jinja result: $result")
    result
  }

  private def logAudit(
    start: Timestamp,
    end: Timestamp,
    jobResultCount: Long,
    success: Boolean,
    message: String,
    test: Boolean
  ): Unit = {
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
      AuditLog.sink(log)
    }
  }

  def logAuditSuccess(start: Timestamp, end: Timestamp, jobResultCount: Long, test: Boolean): Unit =
    logAudit(start, end, jobResultCount, success = true, "success", test)

  def logAuditFailure(start: Timestamp, end: Timestamp, e: Throwable, test: Boolean): Unit =
    logAudit(start, end, -1, success = false, Utils.exceptionAsString(e), test)

  def dependencies(): List[String] = {
    val result = SQLUtils.extractRefsInFromAndJoin(parseJinja(taskDesc.getSql(), Map.empty))
    logger.info(s"$name has ${result.length} dependencies: ${result.mkString(",")}")
    result
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
}

object AutoTask extends StrictLogging {
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
          logExecution = true
        )
      )
  }

  def task(
    appId: Option[String],
    taskDesc: AutoTaskDesc,
    configOptions: Map[String, String],
    interactive: Option[String],
    truncate: Boolean,
    test: Boolean,
    engine: Engine,
    logExecution: Boolean,
    accessToken: Option[String] = None,
    resultPageSize: Int = 1000,
    dryRun: Boolean = false
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
          resultPageSize = resultPageSize
        )
      case _ =>
        sinkConfig match {
          case fs: FsSink if fs.isExport() && interactive.isEmpty =>
            logger.info("Exporting to the filesystem")
            new SparkExportTask(
              appId,
              taskDesc,
              configOptions,
              interactive,
              truncate = truncate,
              test = test,
              accessToken = accessToken,
              resultPageSize = resultPageSize,
              logExecution = logExecution
            )

          case _ =>
            new SparkAutoTask(
              appId,
              taskDesc,
              configOptions,
              interactive,
              truncate = truncate,
              test = test,
              accessToken = accessToken,
              resultPageSize = resultPageSize,
              logExecution = logExecution
            )
        }
    }
  }

  def query(
    domain: String,
    table: String,
    sql: String,
    summarizeOnly: Boolean,
    connectionName: String
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[List[Map[String, String]]] = Try {
    val connection = settings.appConfig
      .connection(connectionName)
      .getOrElse(throw new Exception(s"Connection not found $connectionName"))

    val finalSql =
      if (summarizeOnly)
        if (connection.isDuckDb())
          "SUMMARIZE " + sql
        else
          s"DESCRIBE TABLE $domain.$table"
      else
        sql

    val autoTaskDesc = AutoTaskDesc(
      name = s"$domain.$table",
      sql = Some(finalSql),
      domain = domain,
      table = table,
      database = None,
      connectionRef = Some(connectionName)
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
      test = true,
      engine = engine,
      logExecution = false
    )
    t.run() match {
      case Success(jobResult) =>
        jobResult.asMap()
      case Failure(e) =>
        throw e
    }
  }
}
