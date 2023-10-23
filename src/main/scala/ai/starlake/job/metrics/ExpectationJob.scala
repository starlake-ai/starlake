package ai.starlake.job.metrics

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.utils._
import ai.starlake.utils.conversion.BigQueryUtils
import com.google.cloud.bigquery.TableId
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import scala.util.{Failure, Success, Try}

case class ExpectationReport(
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
  expectations: Map[String, String],
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  inputData: Option[Either[DataFrame, TableId]],
  engine: Engine,
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
    inputData.foreach { dataset =>
      dataset match {
        case Left(df) =>
          df.createOrReplaceTempView("SL_THIS")
        case Right(tableId) =>
          val tableName = BigQueryUtils.tableIdToString(tableId)
          bqSlThisCTE = s"WITH SL_THIS AS (SELECT * FROM $tableName)\n"
      }
    }

    val expectationLibrary = schemaHandler.expectations(domainName)
    val calls = ExpectationCalls(expectations).expectationCalls
    val expectationReports = calls.map { case (_, expectation) =>
      val (sql, assertion) = expectationLibrary
        .get(expectation.name)
        .map { ad =>
          logger.info(s"Applying substitution ${ad.name} -> ${ad.expectation.query}")
          val paramsMap =
            schemaHandler.activeEnvVars() ++ ad.params.zip(expectation.paramValues).toMap
          // Apply substitution defined with {{ }} and overload options in env by option in command line
          val sql = Utils
            .subst(
              Utils
                .parseJinja(ad.expectation.query, schemaHandler.activeEnvVars() ++ paramsMap)
                .richFormat(schemaHandler.activeEnvVars(), paramsMap),
              paramsMap
            )
          val assertion = Utils
            .subst(
              Utils
                .parseJinja(ad.expectation.expect, schemaHandler.activeEnvVars() ++ paramsMap)
                .richFormat(schemaHandler.activeEnvVars(), paramsMap),
              paramsMap
            )
          (bqSlThisCTE + sql, assertion)
        }
        .getOrElse(throw new Exception(s"Expectation ${expectation.name} not found"))
      logger.info(s"Applying expectation ${expectation.name} with request $sql")
      Try {
        val expectationResult = sqlRunner.handle(sql, assertion)
        ExpectationReport(
          expectation.name,
          expectation.paramValues.toString(),
          Some(sql),
          Some(expectationResult("count").asInstanceOf[Long]),
          None,
          success = expectationResult("assertion").asInstanceOf[Boolean]
        )
      } match {
        case Failure(e: IllegalArgumentException) =>
          e.printStackTrace()
          ExpectationReport(
            expectation.name,
            expectation.paramValues.toString(),
            None,
            None,
            Some(Utils.exceptionAsString(e)),
            success = false
          )
        case Failure(e) =>
          e.printStackTrace()
          ExpectationReport(
            expectation.name,
            expectation.paramValues.toString(),
            Some(sql),
            None,
            Some(Utils.exceptionAsString(e)),
            success = false
          )
          throw new Exception(e)
        case Success(value) => value
      }
    }.toList
    if (expectationReports.nonEmpty) {
      expectationReports.foreach(r => logger.info(r.toString))
      val expectationsDF = session
        .createDataFrame(expectationReports)
        .withColumn("jobId", lit(applicationId()))
        .withColumn("database", lit(database.getOrElse("")))
        .withColumn("domain", lit(domainName))
        .withColumn("schema", lit(schemaName))
        .withColumn("timestamp", lit(System.currentTimeMillis()))

      new SinkUtils().sinkInAudit(
        settings.appConfig.audit.sink.getSink().getConnectionType(),
        expectationsDF,
        "expectations",
        Some("Expectation results"),
        DatasetArea.expectations(domainName, schemaName),
        lockPath(settings.appConfig.expectations.path),
        storageHandler,
        engine,
        session
      )
    }
    val failed = expectationReports.count(!_.success)
    if (settings.appConfig.expectations.failOnError && failed > 0) {
      Failure(new Exception(s"$failed Expectations failed"))
    } else {
      Success(SparkJobResult(None))
    }
  }
}
