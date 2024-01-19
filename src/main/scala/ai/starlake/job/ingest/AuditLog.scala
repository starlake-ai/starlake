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
import ai.starlake.job.sink.bigquery.BigQueryJobBase
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.{JobResult, Utils}
import com.google.cloud.MonitoredResource
import com.google.cloud.bigquery.StandardSQLTypeName
import com.google.cloud.logging.Payload.JsonPayload
import com.google.cloud.logging.{LogEntry, LoggingOptions}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.util.Collections
import java.util.regex.Pattern
import scala.util.{Success, Try}
import scala.collection.JavaConverters._

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
  duration: Long,
  message: String,
  step: String,
  database: Option[String],
  tenant: String
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

  def sink(log: AuditLog)(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[JobResult] = {
    if (settings.appConfig.audit.isActive()) {
      val auditSink = settings.appConfig.audit.getSink()
      auditSink.getConnectionType() match {
        case ConnectionType.GCPLOG =>
          logToGCP(log)
          Success(new JobResult {})
        case _ =>
          val selectSql =
            log.asSelect(auditSink.getConnection().getJdbcEngineName())
          val auditTaskDesc = AutoTaskDesc(
            name = s"audit-${log.jobid}",
            sql = Some(selectSql),
            database = settings.appConfig.audit.getDatabase(),
            domain = settings.appConfig.audit.getDomain(),
            table = "audit",
            write = Some(WriteMode.APPEND),
            partition = Nil,
            presql = Nil,
            postsql = Nil,
            sink = Some(settings.appConfig.audit.sink),
            parseSQL = Some(false),
            _auditTableName = Some("audit"),
            taskTimeoutMs = Some(settings.appConfig.shortJobTimeoutMs)
          )
          val task = AutoTask
            .task(
              auditTaskDesc,
              Map.empty,
              None,
              truncate = false
            )
          val res = task.run()
          Utils.logFailure(res, logger)
      }
    } else {
      Success(new JobResult {})
    }
  }

  private def logToGCP(log: AuditLog)(implicit
    settings: Settings
  ): Unit = {
    val logName = settings.appConfig.audit.getDomain()
    val logging = LoggingOptions.getDefaultInstance
      .toBuilder()
      .setProjectId(BigQueryJobBase.projectId(None))
      .build()
      .getService
    try {
      val entry = LogEntry
        .newBuilder(JsonPayload.of(log.asMap().asJava))
        .setSeverity(com.google.cloud.logging.Severity.INFO)
        .setLogName(logName)
        .setResource(MonitoredResource.newBuilder("global").build)
        .build
      // Writes the log entry asynchronously
      logging.write(Collections.singleton(entry))
      // Optional - flush any pending log entries just before Logging is closed
      logging.flush()
    } finally if (logging != null) logging.close()
  }
}
