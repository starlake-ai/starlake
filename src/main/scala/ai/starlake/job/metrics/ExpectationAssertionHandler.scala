package ai.starlake.job.metrics

import ai.starlake.job.sink.bigquery.BigQueryNativeJob
import ai.starlake.utils.CompilerUtils
import ai.starlake.utils.conversion.BigQueryUtils
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter
import scala.util.{Failure, Success}

trait ExpectationAssertionHandler extends StrictLogging {
  def handle(sql: String, assertion: String): Map[String, Any]

  protected def submitExpectation(
    assertion: String,
    count: Long,
    result: Seq[Any],
    results: Seq[Seq[Any]]
  ): Map[String, Any] = {
    val context = Map("count" -> count, "result" -> result, "results" -> results)
    logger.info(s"Submitting expectation: $assertion with context $context")
    val assertionCode = assertion
    val assertionFunction = CompilerUtils.compile[java.lang.Boolean](assertionCode)
    val assertionResult = assertionFunction(context)
    context ++ Map("assertion" -> assertionResult)
  }

}

class SparkExpectationAssertionHandler(session: SparkSession) extends ExpectationAssertionHandler {
  def handle(sql: String, assertion: String): Map[String, Any] = {
    val df = session.sql(sql)
    val count = df.count()
    val result = if (df.count() == 1) {
      val row = df.collect().head
      row.toSeq
    } else {
      null
    }
    val results = if (df.count() > 1) {
      val rows = df.collect()
      rows.map(row => row.toSeq).toSeq
    } else
      null
    submitExpectation(assertion, count, result, results)
  }
}

/** result.iterateAll().forEach(rows -> rows.forEach(row -> System.out.println(row.getValue())));
  *
  * @param runner
  */
class BigQueryExpectationAssertionHandler(runner: BigQueryNativeJob)
    extends ExpectationAssertionHandler {
  override def handle(sql: String, assertion: String): Map[String, Any] = {
    runner
      .runInteractiveQuery(Some(sql)) match {
      case Success(result) =>
        result.tableResult
          .map { tableResult =>
            val count = tableResult.getTotalRows
            val schema = tableResult.getSchema.getFields.asScala.map(_.getType()).toSeq
            val result = if (count == 1) {
              val values = tableResult.iterateAll().asScala.head.asScala.toSeq
              BigQueryUtils.anyRefToAny(values, schema)
            } else {
              null
            }
            val results = if (count > 1) {
              tableResult
                .iterateAll()
                .asScala
                .map { row =>
                  val values = row.asScala.toSeq
                  BigQueryUtils.anyRefToAny(values, schema)
                }
                .toSeq
            } else {
              null
            }
            submitExpectation(assertion, count, result, results)
          }
          .getOrElse(
            throw new Exception("Query did not return result object. Should never happen !!!")
          )
      case Failure(e) =>
        throw e
    }
  }

}
