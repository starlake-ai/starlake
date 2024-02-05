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
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils._
import com.typesafe.scalalogging.StrictLogging

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
  val truncate: Boolean = false,
  val resultPageSize: Int = 1
)(implicit val settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends SparkJob {

  val sinkConfig = taskDesc.getSinkConfig()

  def fullTableName: String

  def run(): Try[JobResult]
  // def buildMergeStatement(partitionColumn: String, destinationTable: String): String

  // def sink(maybeDataFrame: Option[DataFrame]): Boolean

  override def name: String = taskDesc.name

  protected val sinkConnectionRef: String =
    sinkConfig.flatMap(_.connectionRef).getOrElse(settings.appConfig.connectionRef)

  protected val sinkConnection: Settings.Connection =
    settings.appConfig.connections(sinkConnectionRef)

  protected val runEngine: Engine = taskDesc.getEngine()

  protected def strategy: StrategyOptions = taskDesc.getStrategy()

  protected def isMerge(sql: String): Boolean = {
    sql.toLowerCase().contains("merge into")
  }

  protected def tableExists: Boolean

  protected lazy val allVars =
    schemaHandler.activeEnvVars() ++ commandParameters ++ Map("merge" -> tableExists)
  protected lazy val preSql = parseJinja(taskDesc.presql, allVars).filter(_.trim.nonEmpty)
  protected lazy val postSql = parseJinja(taskDesc.postsql, allVars).filter(_.trim.nonEmpty)

  val jdbcSinkEngineName = this.sinkConnection.getJdbcEngineName()
  val jdbcSinkEngine = settings.appConfig.jdbcEngines(jdbcSinkEngineName.toString)

  def substituteRefTaskMainSQL(sql: String) = {
    val selectStatement = Utils.parseJinja(sql, allVars)
    val select =
      SQLUtils.substituteRefInSQLSelect(
        selectStatement,
        schemaHandler.refs(),
        schemaHandler.domains(),
        schemaHandler.tasks(),
        taskDesc.getSinkConnection()
      )
    select
  }

  def buildAllSQLQueries(sql: Option[String]): String = {
    throw new Exception("Implemented in subclasses only")
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
    message: String
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
        settings.appConfig.tenant
      )
      AuditLog.sink(log)
    }
  }

  def logAuditSuccess(start: Timestamp, end: Timestamp, jobResultCount: Long): Unit =
    logAudit(start, end, jobResultCount, success = true, "success")

  def logAuditFailure(start: Timestamp, end: Timestamp, e: Throwable): Unit =
    logAudit(start, end, -1, success = false, Utils.exceptionAsString(e))

  def dependencies(): List[String] = {
    val result = SQLUtils.extractRefsInFromAndJoin(parseJinja(taskDesc.getSql(), Map.empty))
    logger.info(s"$name has ${result.length} dependencies: ${result.mkString(",")}")
    result
  }

  val (createDisposition, writeDisposition) =
    Utils.getDBDisposition(
      taskDesc.getWrite()
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
      .map(task(_, Map.empty, None, engine = Engine.SPARK, truncate = false))
  }

  def task(
    taskDesc: AutoTaskDesc,
    configOptions: Map[String, String],
    interactive: Option[String],
    truncate: Boolean,
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
          resultPageSize = resultPageSize
        )
      case Engine.JDBC =>
        new JdbcAutoTask(
          taskDesc,
          configOptions,
          interactive,
          truncate = truncate,
          resultPageSize = resultPageSize
        )
      case _ =>
        new SparkAutoTask(
          taskDesc,
          configOptions,
          interactive,
          truncate = truncate,
          resultPageSize = resultPageSize
        )
    }
  }
}
