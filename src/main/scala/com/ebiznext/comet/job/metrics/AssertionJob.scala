package com.ebiznext.comet.job.metrics

import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.Formatter._
import com.ebiznext.comet.utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import scala.util.{Failure, Success, Try}

case class AssertionReport(
                            name: String,
                            params: String,
                            sql: Option[String],
                            countFailed: Option[Long],
                            message: Option[String],
                            success: Boolean
                          ) {

  override def toString: String = {
    s"""name: $name, params:$params, countFailed:${
      countFailed.getOrElse(
        0
      )
    }, success:$success, message: ${message.getOrElse("")}, sql:$sql""".stripMargin
  }
}

/** Record assertion execution
 */

/** @param domain        : Domain name
 * @param schema         : Schema
 * @param stage          : stage
 * @param storageHandler : Storage Handler
 */
class AssertionJob(
                    domainName: String,
                    schemaName: String,
                    assertions: Map[String, String],
                    stage: Stage,
                    storageHandler: StorageHandler,
                    schemaHandler: SchemaHandler,
                    dataset: Option[DataFrame],
                    engine: Engine,
                    sqlRunner: String => Long
                  )(implicit val settings: Settings)
  extends SparkJob {

  override def name: String = "Check Assertions"

  def lockPath(path: String): Path = {
    new Path(
      settings.comet.lock.path,
      "assertions" + path
        .replace("{domain}", domainName)
        .replace("{schema}", schemaName)
        .replace('/', '_') + ".lock"
    )
  }

  override def run(): Try[JobResult] = {
    val count = dataset.map { dataset =>
      dataset.createOrReplaceTempView("comet_table")
      dataset.count()
    } getOrElse 0

    val assertionLibrary = schemaHandler.assertions(domainName)
    val calls = AssertionCalls(assertions).assertionCalls
    val assertionReports = calls.map { case (_, assertion) =>
      val sql = assertionLibrary
        .get(assertion.name)
        .map { ad =>
          val paramsMap = schemaHandler.activeEnv ++ ad.params.zip(assertion.paramValues).toMap
          // Apply substitution defined with {{ }} and overload options in env by option in command line
          Utils
            .subst(ad.sql.richFormat(paramsMap), paramsMap)
        }
        .getOrElse(assertion.sql)
      Try {
        val assertionCount = sqlRunner(sql)
        AssertionReport(
          assertion.name,
          assertion.paramValues.toString(),
          Some(sql),
          Some(assertionCount),
          None,
          true
        )
      } match {
        case Failure(e: IllegalArgumentException) =>
          AssertionReport(
            assertion.name,
            assertion.paramValues.toString(),
            None,
            None,
            Some(Utils.exceptionAsString(e)),
            false
          )
        case Failure(e) =>
          AssertionReport(
            assertion.name,
            assertion.paramValues.toString(),
            Some(sql),
            None,
            Some(Utils.exceptionAsString(e)),
            false
          )
        case Success(value) => value
      }
    }.toList
    if (assertionReports.nonEmpty) {
      assertionReports.foreach(r => logger.info(r.toString))

      val assertionsDF = session
        .createDataFrame(assertionReports)
        .withColumn("jobId", lit(session.sparkContext.applicationId))
        .withColumn("domain", lit(domainName))
        .withColumn("schema", lit(schemaName))
        .withColumn("count", lit(count))
        .withColumn("cometTime", lit(System.currentTimeMillis()))
        .withColumn("cometStage", lit(Stage.UNIT.value))

      val assertionsResult =
        if (engine == Engine.SPARK && settings.comet.sinkToFile) {
          val savePath: Path = DatasetArea.assertions(domainName, schemaName)
          val lockedPath = lockPath(settings.comet.assertions.path)
          val waitTimeMillis = settings.comet.lock.metricsTimeout
          val locker = new FileLock(lockedPath, storageHandler)
          locker.tryExclusively(waitTimeMillis) {
            appendToFile(storageHandler, assertionsDF, savePath)
          }
        } else
          Success(None)

      val assertionSinkResult = new SinkUtils().sink(
        settings.comet.assertions.sink,
        assertionsDF,
        settings.comet.assertions.sink.name.getOrElse("assertions")
      )
      for {
        _ <- assertionsResult
        _ <- assertionSinkResult
      } yield {
        None
      }
    }
    Success(SparkJobResult(None))
  }
}
