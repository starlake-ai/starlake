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

package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.common.TaskSQLStatements
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.{GcpUtils, JobResult, Utils}
import com.google.cloud.bigquery.StandardSQLTypeName
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.util.regex.Pattern
import scala.util.{Success, Try}

sealed case class Step(value: String) {
  override def toString: String = value
}

object Step {

  def fromString(value: String): Step = {
    value.toUpperCase() match {
      case "LOAD"      => Step.LOAD
      case "TRANSFORM" => Step.TRANSFORM
    }
  }

  object LOAD extends Step("LOAD")

  object TRANSFORM extends Step("TRANSFORM")

  val steps: Set[Step] = Set(LOAD, TRANSFORM)
}

case class AuditLog(
  jobid: String,
  paths: Option[String],
  domain: String,
  schema: String,
  success: Boolean,
  count: Long,
  countAccepted: Long,
  countRejected: Long,
  timestamp: Timestamp,
  duration: Long, // in detailed load audit, duration represent the duration at which the log is created and not the duration of the ingestion process. To retrieve the ingestion process duration, it is the max of the column.
  message: String,
  step: String,
  database: Option[String],
  tenant: String,
  test: Boolean
) {

  def asMap(): Map[String, Any] = {
    Map(
      "jobid"         -> jobid,
      "domain"        -> domain,
      "schema"        -> schema,
      "success"       -> success,
      "count"         -> count,
      "countAccepted" -> countAccepted,
      "countRejected" -> countRejected,
      "timestamp"     -> timestamp.getTime,
      "duration"      -> duration,
      "message"       -> message,
      "step"          -> step,
      "tenant"        -> tenant
    ) ++ List(
      paths.map("paths" -> _),
      database.map("database" -> _)
    ).flatten
  }

  def asSelect(engineName: Engine)(implicit settings: Settings): String = {
    import ai.starlake.utils.Formatter._
    timestamp.setNanos(0)
    val template: String = AuditLog.selectTemplate(engineName)
    val selectStatement = template.richFormat(
      Map(
        "jobid"         -> jobid,
        "paths"         -> paths.map(p => p.replaceAll("'", "-")).getOrElse("null"),
        "domain"        -> domain.replaceAll("'", "-"),
        "schema"        -> schema.replaceAll("'", "-"),
        "success"       -> success,
        "count"         -> count,
        "countAccepted" -> countAccepted,
        "countRejected" -> countRejected,
        "timestamp"     -> timestamp.toString(),
        "duration"      -> duration,
        "message"       -> message.replaceAll("'", "-").replaceAll("\n", " "),
        "step"          -> step,
        "database"      -> database.getOrElse(""),
        "tenant"        -> tenant.replaceAll("'", "-")
      ),
      Map.empty
    )
    selectStatement
  }

  override def toString(): String =
    s"""
       |jobid=$jobid
       |paths=${paths.getOrElse("null")}
       |domain=$domain
       |schema=$schema
       |success=$success
       |count=$count
       |countAccepted=$countAccepted
       |countRejected=$countRejected
       |timestamp=$timestamp
       |duration=$duration
       |message=$message
       |step=$step
       |database=$database
       |tenant=$tenant
       |""".stripMargin.split('\n').mkString(",")
}

object AuditLog extends StrictLogging {
  def selectTemplate(engineName: Engine)(implicit settings: Settings): String = {
    val template = settings.appConfig.jdbcEngines
      .get(engineName.toString.toLowerCase())
      .flatMap(_.tables("audit").selectSql)
      .getOrElse("""
             SELECT
               '{{jobid}}' as JOBID,
               '{{paths}}' as PATHS,
               '{{domain}}' as DOMAIN,
               '{{schema}}' as SCHEMA,
               {{success}} as SUCCESS,
               {{count}} as COUNT,
               {{countAccepted}} as COUNTACCEPTED,
               {{countRejected}} as COUNTREJECTED,
               TO_TIMESTAMP('{{timestamp}}') as TIMESTAMP,
               {{duration}} as DURATION,
               '{{message}}' as MESSAGE,
               '{{step}}' as STEP,
               '{{database}}' as DATABASE,
               '{{tenant}}' as TENANT
           """)
    template
  }

  private val auditCols = List(
    ("jobid", StandardSQLTypeName.STRING, StringType),
    ("paths", StandardSQLTypeName.STRING, StringType),
    ("domain", StandardSQLTypeName.STRING, StringType),
    ("schema", StandardSQLTypeName.STRING, StringType),
    ("success", StandardSQLTypeName.BOOL, BooleanType),
    ("count", StandardSQLTypeName.INT64, LongType),
    ("countAccepted", StandardSQLTypeName.INT64, LongType),
    ("countRejected", StandardSQLTypeName.INT64, LongType),
    ("timestamp", StandardSQLTypeName.TIMESTAMP, TimestampType),
    ("duration", StandardSQLTypeName.INT64, LongType),
    ("message", StandardSQLTypeName.STRING, StringType),
    ("step", StandardSQLTypeName.STRING, StringType),
    ("database", StandardSQLTypeName.STRING, StringType),
    ("tenant", StandardSQLTypeName.STRING, StringType)
  )

  val starlakeSchema = Schema(
    name = "audit",
    pattern = Pattern.compile("ignore"),
    attributes = auditCols.map { case (name, _, dataType) =>
      val tpe = dataType match {
        case StringType  => "string"
        case BooleanType => "boolean"
        case LongType    => "long"
        case TimestampType =>
          "timestamp"
        case _ => throw new RuntimeException(s"Unsupported type $dataType")
      }
      Attribute(name, tpe)
    },
    None,
    None
  )
  def sink(logs: List[AuditLog], accessToken: Option[String])(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[JobResult] = {
    this.createTask(logs, accessToken).map { task =>
      val res = task.run()
      Utils.logFailure(res, logger)
    } match {
      case Some(res) => res
      case None      => Success(new JobResult {})
    }
  }

  def buildListOfSQLStatements(logs: List[AuditLog], accessToken: Option[String])(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Option[TaskSQLStatements] = {
    val auditSink = settings.appConfig.audit.getSink()
    val template = AuditLog.selectTemplate(auditSink.getConnection().getJdbcEngineName()).pyFormat()
    createTask(logs, accessToken).map { task =>
      val statements = task.buildListOfSQLStatements()
      statements.copy(mainSqlIfExists = List(template), mainSqlIfNotExists = null)
    }
  }

  private def createTask(logs: List[AuditLog], accessToken: Option[String])(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Option[AutoTask] = {
    val productionLog = logs.filter(!_.test)
    if (settings.appConfig.audit.isActive() && productionLog.nonEmpty) {
      val auditSink = settings.appConfig.audit.getSink()
      auditSink.getConnectionType() match {
        case ConnectionType.GCPLOG =>
          val logName = settings.appConfig.audit.getDomain()
          // TODO handle gcp log when builing statements
          GcpUtils.sinkToGcpCloudLogging(productionLog.map(_.asMap()), "audit", logName)
          None
        case _ =>
          // TODO: if detailed load audit is set to true and sink is bigquery, we may generate a query that is too long
          // max bigquery size is 1024k characters.
          val jobId = productionLog.headOption
            .map(_.jobid)
            .getOrElse(throw new RuntimeException("No logs found, should not happen"))
          val selectSql = productionLog
            .map(_.asSelect(auditSink.getConnection().getJdbcEngineName()))
            .mkString("\nUNION ALL\n")
          val auditTaskDesc = AutoTaskDesc(
            name = s"audit-$jobId",
            sql = Some(selectSql),
            database = settings.appConfig.audit.getDatabase(),
            domain = settings.appConfig.audit.getDomain(),
            table = "audit",
            presql = Nil,
            postsql = Nil,
            connectionRef = settings.appConfig.audit.sink.connectionRef,
            sink = Some(settings.appConfig.audit.sink),
            parseSQL = Some(true),
            _auditTableName = Some("audit"),
            taskTimeoutMs = Some(settings.appConfig.shortJobTimeoutMs)
          )
          val engine =
            auditTaskDesc.getSinkConnection().isJdbcUrl() match {
              case true =>
                // This handle the case when sparkFormat is true,
                // we do not want to use spark to write the logs
                Engine.JDBC
              case false => auditTaskDesc.getSinkConnection().getEngine()
            }
          val task = AutoTask
            .task(
              appId = Option(jobId),
              taskDesc = auditTaskDesc,
              configOptions = Map.empty,
              interactive = None,
              truncate = false,
              test = false,
              engine = engine,
              logExecution = false, // We do not log the job that write the logs :)
              accessToken = accessToken,
              resultPageSize = 200,
              resultPageNumber = 1,
              dryRun = false
            )
          Some(task)
      }
    } else {
      None
    }
  }
}
