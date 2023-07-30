package ai.starlake.job.metrics

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import scala.util.{Failure, Success, Try}

case class ExpectationReport(
  name: String,
  params: String,
  sql: Option[String],
  count: Option[Long],
  message: Option[String],
  success: Boolean
) {

  override def toString: String = {
    s"""name: $name, params:$params, count:${count.getOrElse(
        0
      )}, success:$success, message: ${message.getOrElse("")}, sql:$sql""".stripMargin
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
  domainName: String,
  schemaName: String,
  expectations: Map[String, String],
  stage: Stage,
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  dataset: Option[DataFrame],
  engine: Engine,
  sqlRunner: String => Long
)(implicit val settings: Settings)
    extends SparkJob {

  override def name: String = "Check Expectations"

  def lockPath(path: String): Path = {
    new Path(
      settings.comet.lock.path,
      "expectations" + path
        .replace("{{domain}}", domainName)
        .replace("{{schema}}", schemaName)
        .replace(":", "_")
        .replace('/', '_') + ".lock"
    )
  }

  override def run(): Try[JobResult] = {
    val count = dataset.map { dataset =>
      dataset.createOrReplaceTempView("comet_table")
      dataset.count()
    } getOrElse 0

    val expectationLibrary = schemaHandler.expectations(domainName)
    val calls = ExpectationCalls(expectations).expectationCalls
    val expectationReports = calls.map { case (_, expectation) =>
      val sql = expectationLibrary
        .get(expectation.name)
        .map { ad =>
          logger.info(s"Applying substitution ${ad.name} -> ${ad.sql}")
          val paramsMap =
            schemaHandler.activeEnvVars() ++ ad.params.zip(expectation.paramValues).toMap
          // Apply substitution defined with {{ }} and overload options in env by option in command line
          Utils
            .subst(
              Utils
                .parseJinja(ad.sql, schemaHandler.activeEnvVars() ++ paramsMap)
                .richFormat(schemaHandler.activeEnvVars(), paramsMap),
              paramsMap
            )
        }
        .getOrElse(expectation.sql)
      logger.info(s"Applying expectation ${expectation.name} with request $sql")
      Try {
        val expectationCount = sqlRunner(sql)
        ExpectationReport(
          expectation.name,
          expectation.paramValues.toString(),
          Some(sql),
          Some(expectationCount),
          None,
          success = true
        )
      } match {
        case Failure(e: IllegalArgumentException) =>
          ExpectationReport(
            expectation.name,
            expectation.paramValues.toString(),
            None,
            None,
            Some(Utils.exceptionAsString(e)),
            success = false
          )
        case Failure(e) =>
          ExpectationReport(
            expectation.name,
            expectation.paramValues.toString(),
            Some(sql),
            None,
            Some(Utils.exceptionAsString(e)),
            success = false
          )
        case Success(value) => value
      }
    }.toList
    if (expectationReports.nonEmpty) {
      expectationReports.foreach(r => logger.info(r.toString))

      val expectationsDF = session
        .createDataFrame(expectationReports)
        .withColumn("jobId", lit(applicationId()))
        .withColumn("domain", lit(domainName))
        .withColumn("schema", lit(schemaName))
        .withColumn("count", lit(count))
        .withColumn("cometTime", lit(System.currentTimeMillis()))
        .withColumn("cometStage", lit(Stage.UNIT.value))

      new SinkUtils().sinkInAudit(
        settings.comet.audit.sink.getSink().getConnectionType(settings),
        expectationsDF,
        "expectations",
        Some("Expectation results"),
        DatasetArea.expectations(domainName, schemaName),
        lockPath(settings.comet.expectations.path),
        storageHandler,
        engine,
        session
      )
    }
    Success(SparkJobResult(None))
  }
}
