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
import ai.starlake.schema.model.Engine.BQ
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils._
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame

import java.sql.Timestamp
import scala.util.Try

object AutoTask extends StrictLogging {
  def unauthenticatedTasks(reload: Boolean)(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): List[AutoTask] = {
    schemaHandler
      .tasks(reload)
      .map(task(_, Map.empty, None, false))
  }

  def task(
    taskDesc: AutoTaskDesc,
    configOptions: Map[String, String],
    interactive: Option[String],
    drop: Boolean,
    resultPageSize: Int = 1
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): AutoTask = {
    taskDesc.getEngine() match {
      case BQ =>
        new BigQueryAutoTask(
          taskDesc,
          configOptions,
          taskDesc.sink
            .map(_.getSink())
            .orElse(Some(AllSinks().getSink())),
          interactive,
          taskDesc.getDatabase(),
          drop = drop,
          resultPageSize = resultPageSize
        )
      case _ =>
        new SparkAutoTask(
          taskDesc,
          configOptions,
          taskDesc.sink
            .map(_.getSink())
            .orElse(Some(AllSinks().getSink())),
          interactive,
          taskDesc.getDatabase(),
          drop = drop,
          resultPageSize = resultPageSize
        )
    }
  }
}

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
  val sinkConfig: Option[Sink],
  val interactive: Option[String],
  val database: Option[String],
  val drop: Boolean = false,
  val resultPageSize: Int = 1
)(implicit val settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends SparkJob {
  override def name: String = taskDesc.name

  def run(): Try[JobResult]

  def sink(maybeDataFrame: Option[DataFrame]): Boolean

  def buildAllSQLQueries(
    tableExists: Boolean,
    localViews: List[String] = Nil // for local development mode only
  ): (List[String], String, List[String]) = {
    // available jinja variables to build sql query depending on
    // whether the table exists or not.
    val allVars = schemaHandler.activeEnvVars() ++ commandParameters ++ Map("merge" -> tableExists)
    val sql = Utils.parseJinja(taskDesc.getSql(), allVars)
    val mergeSql =
      if (!tableExists) {
        if (taskDesc.parseSQL.getOrElse(true))
          SQLUtils.buildSingleSQLQuery(
            sql,
            schemaHandler.refs(),
            schemaHandler.domains(),
            schemaHandler.tasks(),
            localViews,
            taskDesc.getEngine()
          )
        else
          sql
      } else {
        taskDesc.merge match {
          case Some(options) =>
            val mergeSql =
              SQLUtils.buildMergeSql(
                sql,
                options.key,
                taskDesc.getDatabase(),
                taskDesc.domain,
                taskDesc.table,
                taskDesc.getEngine(),
                localViews.nonEmpty
              )
            logger.info(s"Merge SQL: $mergeSql")
            mergeSql
          case None =>
            if (taskDesc.parseSQL.getOrElse(true))
              SQLUtils.buildSingleSQLQuery(
                sql,
                schemaHandler.refs(),
                schemaHandler.domains(),
                schemaHandler.tasks(),
                localViews,
                taskDesc.getEngine()
              )
            else
              sql
        }
      }

    val preSql = parseJinja(taskDesc.presql, allVars)
    val postSql = parseJinja(taskDesc.postsql, allVars)

    (preSql, s"(\n$mergeSql\n)", postSql)
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
  private def parseJinja(sqls: List[String], vars: Map[String, Any]): List[String] = {
    val result = Utils
      .parseJinja(sqls, schemaHandler.activeEnvVars() ++ commandParameters ++ vars)
    logger.info(s"Parse Jinja result: $result")
    result
  }

  private def logAudit(
    start: Timestamp,
    end: Timestamp,
    jobResultCount: Long,
    success: Boolean,
    message: String
  ): Unit = {
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
    AuditLog.sink(optionalAuditSession, log)
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
    Utils.getDBDisposition(taskDesc.getWrite(), hasMergeKeyDefined = false)
}
