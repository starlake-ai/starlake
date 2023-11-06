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
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.{JobResult, Utils}
import com.google.cloud.bigquery.StandardSQLTypeName
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.sql.Timestamp
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
  duration: Long,
  message: String,
  step: String,
  database: Option[String],
  tenant: String
) {
  def asSelect(engineName: Engine)(implicit settings: Settings): String = {
    import ai.starlake.utils.Formatter._
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

  def sink(sessionOpt: Option[SparkSession], log: AuditLog)(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[JobResult] = {
    if (settings.appConfig.audit.isActive()) {
      val selectSql =
        log.asSelect(settings.appConfig.audit.getSink().getConnection().getJdbcEngineName())
      val auditTaskDesc = AutoTaskDesc(
        name = s"audit-${log.jobid}",
        sql = Some(selectSql),
        database = settings.appConfig.audit.getDatabase(),
        domain = settings.appConfig.audit.domain.getOrElse("audit"),
        table = "audit",
        write = Some(WriteMode.APPEND),
        partition = Nil,
        presql = Nil,
        postsql = Nil,
        sink = Some(settings.appConfig.audit.sink),
        parseSQL = Some(false),
        _auditTableName = Some("audit")
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
    } else {
      Success(new JobResult {})
    }
  }
}
