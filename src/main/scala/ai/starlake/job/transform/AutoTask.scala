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
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import scala.util.Try

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
  val taskDesc: AutoTaskDesc,
  val commandParameters: Map[String, String],
  val interactive: Option[String],
  val test: Boolean,
  val truncate: Boolean = false,
  val resultPageSize: Int = 1
)(implicit val settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends SparkJob {

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

  protected val sinkConnectionRef: String =
    sinkConfig.connectionRef.getOrElse(settings.appConfig.connectionRef)

  protected val sinkConnection: Settings.Connection =
    settings.appConfig.connections(sinkConnectionRef)

  protected val runEngine: Engine = taskDesc.getRunEngine()

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

  val jdbcSinkEngineName = this.sinkConnection.getJdbcEngineName()
  val jdbcSinkEngine = settings.appConfig.jdbcEngines(jdbcSinkEngineName.toString)

  def substituteRefTaskMainSQL(sql: String): String = {
    if (sql.trim.isEmpty)
      sql
    else {
      val selectStatement = Utils.parseJinja(sql, allVars)
      val select =
        SQLUtils.substituteRefInSQLSelect(
          selectStatement,
          schemaHandler.refs(),
          schemaHandler.domains(),
          schemaHandler.tasks(),
          taskDesc.getRunConnection()
        )
      select
    }
  }

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
    tableName: String,
    syncStrategy: TableSync
  ): (List[String], Boolean) = (Nil, true)

  final def buildAllSQLQueries(
    sql: Option[String],
    tableExistsForcedValue: Option[Boolean] = None,
    forceNative: Boolean = false
  ): String = {
    if (taskDesc.parseSQL.getOrElse(true)) {
      val sqlWithParameters = substituteRefTaskMainSQL(sql.getOrElse(taskDesc.getSql()))
      val runConnection =
        if (forceNative) {
          this.taskDesc.getRunConnection().copy(sparkFormat = None)
        } else {
          this.taskDesc.getRunConnection()
        }
      val sqlWithParametersTranspiledIfInTest =
        if (this.test || runConnection._transpileDialect.isDefined) {
          val envVars = schemaHandler.activeEnvVars()
          val timestamps =
            if (this.test) {
              List(
                "SL_CURRENT_TIMESTAMP",
                "SL_CURRENT_DATE",
                "SL_CURRENT_TIME"
              ).flatMap { e =>
                val value = envVars.get(e).orElse(Option(System.getenv().get(e)))
                value.map { v => e -> v }
              }.toMap
            } else
              Map.empty[String, String]
          SQLUtils.transpile(sqlWithParameters, runConnection, timestamps)
        } else
          sqlWithParameters

      val jdbcRunEngineName: Engine = runConnection.getJdbcEngineName()

      val jdbcRunEngine = settings.appConfig.jdbcEngines(jdbcRunEngineName.toString)

      val tblExists =
        tableExistsForcedValue.getOrElse(
          tableExists
        ) // If tableExistsForcedValue is defined, use it, otherwise use tableExists

      val mainSql = StrategiesBuilder(jdbcSinkEngine.strategyBuilder).buildSQLForStrategy(
        strategy,
        sqlWithParametersTranspiledIfInTest,
        fullTableName,
        SQLUtils.extractColumnNames(sqlWithParameters),
        tblExists,
        truncate = truncate,
        materializedView = isMaterializedView(),
        jdbcRunEngine,
        sinkConfig
      )
      if (settings.appConfig.syncSqlWithYaml && taskDesc._auditTableName.isEmpty) {
        val list = schemaHandler.syncPreviewSqlWithYaml(taskDesc.name, None)
        schemaHandler.syncApplySqlWithYaml(taskDesc, list, None)
      }
      if (
        this.taskDesc.readyForSync() &&
        settings.appConfig.syncYamlWithDb &&
        taskDesc._auditTableName.isEmpty
      ) {
        logger.info(s"Main SQL: $mainSql")
        logger.info("Identifying new / altered columns for " + fullTableName)
        val columnStatements =
          if (tableExists) {
            val (columnStatements, _) =
              buildTableSchemaSQL(
                this.taskDesc.sparkSchema(schemaHandler),
                this.fullTableName,
                this.taskDesc.getSyncStrategyValue()
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
      val selectStatement = Utils.parseJinja(sql.getOrElse(taskDesc.getSql()), allVars)
      selectStatement
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
  protected def parseJinja(sqls: List[String], vars: Map[String, Any]): List[String] = {
    val result = Utils
      .parseJinja(sqls, schemaHandler.activeEnvVars() ++ commandParameters ++ vars)
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

  def isMaterializedView(): Boolean = {
    taskDesc.sink.flatMap(_.materializedView).getOrElse(false)
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
      .map(task(_, Map.empty, None, engine = Engine.SPARK, truncate = false, test = false))
  }

  def task(
    taskDesc: AutoTaskDesc,
    configOptions: Map[String, String],
    interactive: Option[String],
    truncate: Boolean,
    test: Boolean,
    engine: Engine,
    resultPageSize: Int = 1
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): AutoTask = {
    engine match {
      case Engine.BQ =>
        new BigQueryAutoTask(
          taskDesc,
          configOptions,
          interactive,
          truncate = truncate,
          test = test,
          resultPageSize = resultPageSize
        )
      case Engine.JDBC =>
        new JdbcAutoTask(
          taskDesc,
          configOptions,
          interactive,
          truncate = truncate,
          test = test,
          resultPageSize = resultPageSize
        )
      case _ =>
        val sinkConfig = taskDesc.getSinkConfig()
        sinkConfig match {
          case fs: FsSink if fs.isExport() =>
            logger.info("Exporting to the filesystem")
            new SparkExportTask(
              taskDesc,
              configOptions,
              interactive,
              truncate = truncate,
              test = test,
              resultPageSize = resultPageSize
            )

          case _ =>
            new SparkAutoTask(
              taskDesc,
              configOptions,
              interactive,
              truncate = truncate,
              test = test,
              resultPageSize = resultPageSize
            )

        }
    }
  }
}
