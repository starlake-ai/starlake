package ai.starlake.job.metrics

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.sink.bigquery.BigQueryNativeJob
import ai.starlake.utils.SparkUtils
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success}

trait ExpectationAssertionHandler extends StrictLogging {
  def handle(sql: String)(implicit settings: Settings): Long
}

class SparkExpectationAssertionHandler(session: SparkSession) extends ExpectationAssertionHandler {
  def handle(sql: String)(implicit settings: Settings): Long = {
    val df = SparkUtils.sql(session, sql)
    val count = df.count()
    count
  }
}

/** result.iterateAll().forEach(rows -> rows.forEach(row -> System.out.println(row.getValue())));
  *
  * @param runner
  */
class BigQueryExpectationAssertionHandler(runner: BigQueryNativeJob)
    extends ExpectationAssertionHandler {
  override def handle(sql: String)(implicit
    settings: Settings
  ): Long = {
    runner.runInteractiveQuery(Some(sql)) match {
      case Success(result) =>
        result.tableResult
          .map { tableResult =>
            val count = tableResult.getTotalRows
            count
          }
          .getOrElse(
            throw new Exception("Query did not return result object. Should never happen !!!")
          )
      case Failure(e) =>
        throw e
    }
  }

}

/** result.iterateAll().forEach(rows -> rows.forEach(row -> System.out.println(row.getValue())));
  *
  * @param runner
  */
class JdbcExpectationAssertionHandler(jdbcProperties: Map[String, String])
    extends ExpectationAssertionHandler {
  override def handle(sql: String)(implicit
    settings: Settings
  ): Long = {
    JdbcDbUtils.withJDBCConnection(jdbcProperties) { connection =>
      val statement = connection.createStatement()
      try {
        val rs = statement.executeQuery(sql)

        val count =
          if (rs != null && rs.next()) {
            // get row count
            try {
              rs.last()
              rs.getRow
            } catch {
              // Some drivrs don't support rs.last() and throw an exception
              case _: Throwable =>
                Integer.MAX_VALUE
            }
          } else
            0
        count
      } finally {
        statement.close()
      }
    }
  }
}
