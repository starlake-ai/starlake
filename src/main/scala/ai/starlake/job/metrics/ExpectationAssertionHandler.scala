package ai.starlake.job.metrics

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.sink.bigquery.BigQueryNativeJob
import ai.starlake.utils.SparkUtils
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

trait ExpectationAssertionHandler extends LazyLogging {
  def handle(sql: String)(implicit settings: Settings): Int
}

class SparkExpectationAssertionHandler(session: SparkSession) extends ExpectationAssertionHandler {
  def handle(sql: String)(implicit settings: Settings): Int = {
    val df = SparkUtils.sql(session, sql)
    val count = df.count()
    if (df.count() == 1)
      Try(df.collect().head.getInt(0))
        .getOrElse(Integer.MIN_VALUE)
    else // More than one line, this is a mistake
      Integer.MIN_VALUE
  }
}

class BigQueryExpectationAssertionHandler(runner: BigQueryNativeJob)
    extends ExpectationAssertionHandler {
  override def handle(sql: String)(implicit
    settings: Settings
  ): Int = {
    runner.runBigQueryJob(Some(sql)) match {
      case Success(result) =>
        result.tableResult
          .map { tableResult =>
            if (tableResult.hasNextPage) {
              val iterator = tableResult.getValues.iterator()
              if (iterator.hasNext) {
                val field = iterator.next()
                if (iterator.hasNext) // More than one line this is a mistake!
                  Integer.MIN_VALUE
                else
                  Try(field.get(0).getLongValue.toInt).getOrElse(Integer.MIN_VALUE)
              } else
                Integer.MIN_VALUE
            } else {
              Integer.MIN_VALUE
            }
          }
          .getOrElse(
            throw new Exception("Query did not return result object. Should never happen !!!")
          )
      case Failure(e) =>
        throw e
    }
  }
}

class JdbcExpectationAssertionHandler(jdbcProperties: Map[String, String])
    extends ExpectationAssertionHandler {
  override def handle(sql: String)(implicit
    settings: Settings
  ): Int = {
    JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), jdbcProperties) {
      connection =>
        val statement = connection.createStatement()
        try {
          val rs = statement.executeQuery(sql)
          if (rs != null && rs.next()) {
            val returnValue = Try(rs.getInt(1)).getOrElse(Integer.MIN_VALUE)
            if (rs.next()) // More than one line this is a mistake
              Integer.MIN_VALUE
            else
              returnValue

          } else
            Integer.MIN_VALUE
        } finally {
          statement.close()
        }
    }
  }
}

class SQLConnectionExpectationAssertionHandler(connection: java.sql.Connection)
    extends ExpectationAssertionHandler {
  override def handle(sql: String)(implicit
    settings: Settings
  ): Int = {
    val statement = connection.createStatement()
    try {
      val rs = statement.executeQuery(sql)
      if (rs != null && rs.next()) {
        val returnValue = Try(rs.getInt(1)).getOrElse(Integer.MIN_VALUE)
        if (rs.next()) // More than one line this is a mistake
          Integer.MIN_VALUE
        else
          returnValue

      } else
        Integer.MIN_VALUE
    } finally {
      statement.close()
    }
  }
}
