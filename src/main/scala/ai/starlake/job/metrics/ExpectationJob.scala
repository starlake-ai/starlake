package ai.starlake.job.metrics

import ai.starlake.config.Settings
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils._
import org.apache.hadoop.fs.Path

import java.sql.Timestamp
import java.time.Instant
import java.util.regex.Pattern
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
    val tableName = database match {
      case Some(db) => s"$db.$domainName.$schemaName"
      case None     => s"$domainName.$schemaName"
    }
    bqSlThisCTE = s"WITH SL_THIS AS (SELECT * FROM $tableName)\n"

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
        s"Applying expectation ${expectation.name}: ${expectation.query} with request $sql"
      )
      Try {
        val expectationResult = sqlRunner.handle(sql, assertion)
        ExpectationReport(
          applicationId(),
          database,
          domainName,
          schemaName,
          Timestamp.from(Instant.now()),
          expectation.name.getOrElse(""),
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
            expectation.name.getOrElse(""),
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
            expectation.name.getOrElse(""),
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
    val result = if (expectationReports.nonEmpty) {
      expectationReports.foreach(r => logger.info(r.toString))

      val sqls = expectationReports
        .map(
          _.asSelect(settings.appConfig.audit.sink.getSink().getConnection().getJdbcEngineName())
        )
        .mkString("", " UNION ", "")
      val taskDesc = AutoTaskDesc(
        name = s"audit-${applicationId()}",
        sql = Some(sqls),
        database = settings.appConfig.audit.getDatabase(),
        domain = settings.appConfig.audit.getDomain(),
        table = "expectations",
        write = Some(WriteMode.APPEND),
        partition = Nil,
        presql = Nil,
        postsql = Nil,
        sink = Some(settings.appConfig.audit.sink),
        parseSQL = Some(true),
        _auditTableName = Some("expectations")
      )
      val task = AutoTask
        .task(
          taskDesc,
          Map.empty,
          None,
          truncate = false,
          test = false,
          engine = taskDesc.getSinkConnection().getEngine()
        )(settings, storageHandler, schemaHandler)
      val res = task.run()
      Utils.logFailure(res, logger)
    } else
      Success(SparkJobResult(None))
    val failed = expectationReports.count(!_.success)
    if (settings.appConfig.expectations.failOnError && failed > 0) {
      Failure(new Exception(s"$failed Expectations failed"))
    } else {
      result
    }
  }
}
