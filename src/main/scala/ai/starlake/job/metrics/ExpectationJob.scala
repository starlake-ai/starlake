package ai.starlake.job.metrics

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.BigQueryJobBase
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils._
import com.google.cloud.MonitoredResource
import com.google.cloud.logging.Payload.JsonPayload
import com.google.cloud.logging.{LogEntry, LoggingOptions}
import org.apache.hadoop.fs.Path

import java.sql.Timestamp
import java.time.Instant
import java.util.Collections
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

case class ExpectationReport(
  jobId: String,
  database: Option[String],
  domain: String,
  schema: String,
  timestamp: Timestamp,
  name: String,
  params: String,
  sql: Option[String],
  count: Option[Long],
  exception: Option[String],
  success: Boolean
) {

  def asMap(): Map[String, Any] = {
    Map(
      "jobid"     -> jobId,
      "database"  -> database.getOrElse(""),
      "domain"    -> domain,
      "schema"    -> schema,
      "timestamp" -> timestamp,
      "name"      -> name,
      "params"    -> params,
      "sql"       -> sql.getOrElse(""),
      "count"     -> count.getOrElse(0),
      "exception" -> exception.getOrElse(""),
      "success"   -> success
    )
  }

  override def toString: String = {
    s"""name: $name, params:$params, count:${count.getOrElse(
        0
      )}, success:$success, message: ${exception.getOrElse("")}, sql:$sql""".stripMargin
  }

  def asSelect(engineName: Engine)(implicit settings: Settings): String = {
    import ai.starlake.utils.Formatter._
    timestamp.setNanos(0)
    val template = settings.appConfig.jdbcEngines
      .get(engineName.toString)
      .flatMap(_.tables("expectations").selectSql)
      .getOrElse("""
         |SELECT
         |'{{jobid}}' as JOBID,
         |'{{database}}' as DATABASE,
         |'{{domain}}' as DOMAIN,
         |'{{schema}}' as SCHEMA,
         |TO_TIMESTAMP('{{timestamp}}') as TIMESTAMP,
         |'{{name}}' as NAME,
         |'{{params}}' as PARAMS,
         |'{{sql}}' as SQL,
         |{{count}} as COUNT,
         |'{{exception}}' as EXCEPTION,
         |{{success}} as SUCCESS
         """.stripMargin)
    val selectStatement = template.richFormat(
      Map(
        "jobid"     -> jobId,
        "database"  -> database.getOrElse(""),
        "domain"    -> domain,
        "schema"    -> schema,
        "timestamp" -> timestamp.toString(),
        "name"      -> name,
        "params"    -> params.replaceAll("'", "-").replaceAll("\n", " "),
        "sql"       -> sql.getOrElse("").replaceAll("'", "-").replaceAll("\n", " "),
        "count"     -> count.getOrElse(0L).toString,
        "exception" -> exception.getOrElse("").replaceAll("'", "-").replaceAll("\n", " "),
        "success"   -> success.toString
      ),
      Map.empty
    )
    selectStatement
  }
}

object ExpectationReport {
  val starlakeSchema = Schema(
    name = "expectations",
    pattern = Pattern.compile("ignore"),
    attributes = List(
      Attribute("jobid", "string"),
      Attribute("database", "string"),
      Attribute("domain", "string"),
      Attribute("schema", "string"),
      Attribute("timestamp", "timestamp"),
      Attribute("query", "string"),
      Attribute("expect", "string"),
      Attribute("sql", "string"),
      Attribute("count", "long"),
      Attribute("exception", "string"),
      Attribute("success", "boolean")
    ),
    None,
    None
  )
}

/** Record expectation execution
  */

/** @param domain
  *   : Domain name
  * @param schema
  *   : Schema
  * @param stage
  *   : stage
  * @param storageHandler
  *   : Storage Handler
  */
class ExpectationJob(
  appId: Option[String],
  database: Option[String],
  domainName: String,
  schemaName: String,
  expectations: List[ExpectationItem],
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  sqlRunner: ExpectationAssertionHandler
)(implicit val settings: Settings)
    extends SparkJob {

  override def name: String = "Check Expectations"

  override def applicationId(): String = this.appId.getOrElse(this.applicationId())

  def lockPath(path: String): Path = {
    new Path(
      settings.appConfig.lock.path,
      "expectations" + path
        .replace("{{domain}}", domainName)
        .replace("{{schema}}", schemaName)
        .replace(":", "_")
        .replace('/', '_') + ".lock"
    )
  }

  override def run(): Try[JobResult] = {
    var bqSlThisCTE = ""
    val fullTableName = database match {
      case Some(db) => s"$db.$domainName.$schemaName"
      case None     => s"$domainName.$schemaName"
    }
    bqSlThisCTE = s"WITH SL_THIS AS (SELECT * FROM $fullTableName)\n"

    val macros = schemaHandler.jinjavaMacros
    val expectationReports = expectations.map { expectation =>
      val expectationWithMacroDefinitions = List(macros, expectation.queryCall()).mkString("\n")
      val sql = bqSlThisCTE +
        Utils.parseJinja(
          expectationWithMacroDefinitions,
          schemaHandler.activeEnvVars()
        )
      val assertion = Utils.parseJinja(expectation.expect, schemaHandler.activeEnvVars())
      logger.info(
        s"Applying expectation: ${expectation.query} with request $sql"
      )
      Try {
        val expectationResult = sqlRunner.handle(sql, assertion)
        ExpectationReport(
          applicationId(),
          database,
          domainName,
          schemaName,
          Timestamp.from(Instant.now()),
          "",
          expectation.query,
          Some(sql),
          Some(expectationResult("count").asInstanceOf[Long]),
          None,
          success = expectationResult("assertion").asInstanceOf[Boolean]
        )
      } match {
        case Failure(e: IllegalArgumentException) =>
          e.printStackTrace()
          ExpectationReport(
            applicationId(),
            database,
            domainName,
            schemaName,
            Timestamp.from(Instant.now()),
            "",
            expectation.query,
            None,
            None,
            Some(Utils.exceptionAsString(e)),
            success = false
          )
        case Failure(e) =>
          e.printStackTrace()
          ExpectationReport(
            applicationId(),
            database,
            domainName,
            schemaName,
            Timestamp.from(Instant.now()),
            "",
            expectation.query,
            Some(sql),
            None,
            Some(Utils.exceptionAsString(e)),
            success = false
          )
          throw new Exception(e)
        case Success(value) => value
      }
    }
    val result =
      if (expectationReports.nonEmpty) {
        expectationReports.foreach(r => logger.info(r.toString))
        val auditSink = settings.appConfig.audit.getSink()
        auditSink.getConnectionType() match {
          case ConnectionType.GCPLOG =>
            expectationReports.foreach(sinkToGcpCloudLogging)
            Success(new JobResult {})
          case _ =>
            val sqls = expectationReports
              .map(
                _.asSelect(
                  settings.appConfig.audit.sink.getSink().getConnection().getJdbcEngineName()
                )
              )
              .mkString("", " UNION ", "")
            val taskDesc = AutoTaskDesc(
              name = applicationId(),
              sql = Some(sqls),
              database = settings.appConfig.audit.getDatabase(),
              domain = settings.appConfig.audit.getDomain(),
              table = "expectations",
              presql = Nil,
              postsql = Nil,
              sink = Some(settings.appConfig.audit.sink),
              parseSQL = Some(true),
              _auditTableName = Some("expectations")
            )
            val task = AutoTask
              .task(
                Option(applicationId()),
                taskDesc,
                Map.empty,
                None,
                truncate = false,
                engine = taskDesc.getSinkConnection().getEngine(),
                test = false
              )(settings, storageHandler, schemaHandler)
            val res = task.run()
            Utils.logFailure(res, logger)
        }
      } else
        Success(SparkJobResult(None, None))
    val failed = expectationReports.count(!_.success)
    if (settings.appConfig.expectations.failOnError && failed > 0) {
      Failure(new Exception(s"$failed Expectations failed"))
    } else {
      result
    }
  }

  private def sinkToGcpCloudLogging(log: ExpectationReport)(implicit
    settings: Settings
  ): Unit = {
    val logName = settings.appConfig.audit.getDomainExpectation()
    val logging = LoggingOptions.getDefaultInstance
      .toBuilder()
      .setProjectId(BigQueryJobBase.projectId(settings.appConfig.audit.database))
      .build()
      .getService
    try {
      val entry = LogEntry
        .newBuilder(JsonPayload.of(log.asMap().asJava))
        .setSeverity(com.google.cloud.logging.Severity.INFO)
        .addLabel("type", "expectation")
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
