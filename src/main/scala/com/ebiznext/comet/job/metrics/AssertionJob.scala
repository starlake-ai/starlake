package com.ebiznext.comet.job.metrics

import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import scala.util.{Success, Try}

case class AssertionReport(
  name: String,
  params: String,
  sql: Option[String],
  countFailed: Option[Long],
  message: Option[String],
  success: Boolean
) {

  override def toString: String = {
    s"""name: $name, params:$params, countFailed:${countFailed.getOrElse(
      0
    )}, success:$success, message: ${message.getOrElse("")}, sql:$sql""".stripMargin
  }
}

/** Record assertion execution
  */

/** @param domain         : Domain name
  * @param schema         : Schema
  * @param stage          : stage
  * @param storageHandler : Storage Handler
  */
class AssertionJob(
  domain: Domain,
  schema: Schema,
  stage: Stage,
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  dataset: DataFrame
)(implicit val settings: Settings)
    extends SparkJob {

  override def name: String = "Check Assertions"

  def lockPath(path: String): Path = {
    new Path(
      settings.comet.lock.path,
      "assertions" + path
        .replace("{domain}", domain.name)
        .replace("{schema}", schema.name)
        .replace('/', '_') + ".lock"
    )
  }

  override def run(): Try[JobResult] = {
    val count = dataset.count()
    schema.assertions.foreach { assertions =>
      dataset.createOrReplaceTempView("comet_table")
      val assertionLibrary = schemaHandler.assertions(domain.name)
      val calls = AssertionCalls(assertions).assertionCalls
      val assertionReports = calls.map { case (_, assertion) =>
        val sql = assertionLibrary
          .get(assertion.name)
          .map(ad => Utils.subst(ad.sql, ad.params, assertion.paramValues, schemaHandler.activeEnv))
          .getOrElse(assertion.sql)
        try {
          val count = session.sql(sql).count()
          AssertionReport(
            assertion.name,
            assertion.paramValues.toString(),
            Some(sql),
            Some(count),
            None,
            true
          )
        } catch {
          case e: IllegalArgumentException =>
            AssertionReport(
              assertion.name,
              assertion.paramValues.toString(),
              None,
              None,
              Some(Utils.exceptionAsString(e)),
              false
            )
          case e: Exception =>
            AssertionReport(
              assertion.name,
              assertion.paramValues.toString(),
              Some(sql),
              None,
              Some(Utils.exceptionAsString(e)),
              false
            )
        }
      }.toList

      assertionReports.foreach(r => logger.info(r.toString))

      val assertionsDF = session
        .createDataFrame(assertionReports)
        .withColumn("jobId", lit(session.sparkContext.applicationId))
        .withColumn("domain", lit(domain.name))
        .withColumn("schema", lit(schema.name))
        .withColumn("count", lit(count))
        .withColumn("cometTime", lit(System.currentTimeMillis()))
        .withColumn("cometStage", lit(Stage.UNIT.value))

      val savePath: Path = DatasetArea.assertions(domain.name, schema.name)
      val lockedPath = lockPath(settings.comet.assertions.path)
      val waitTimeMillis = settings.comet.lock.metricsTimeout
      val locker = new FileLock(lockedPath, storageHandler)
      val assertionsResult = locker.tryExclusively(waitTimeMillis) {
        appendToFile(storageHandler, assertionsDF, new Path(savePath, savePath))
      }

      val assertionSinkResult = new SinkUtils().sinkMetrics(
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
