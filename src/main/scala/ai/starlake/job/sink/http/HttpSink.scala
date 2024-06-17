package ai.starlake.job.sink.http

import ai.starlake.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

import scala.util.{Failure, Success, Try}

class HtttpSink(parameters: Map[String, String]) extends Sink with StrictLogging {
  private val numRetries: Int = parameters.getOrElse("numRetries", "3").toInt
  private val retryInterval: Int = parameters.getOrElse("retryInterval", "1000").toInt

  val client = new HttpSinkClient(parameters)

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    var success = false;
    var retried = 0;
    while (!success && retried < numRetries) {
      Try {
        retried += 1;
        client.send(data);
        success = true;
      } match {
        case Failure(e) =>
          success = false;
          logger.warn(Utils.exceptionAsString(e));
          if (retried < numRetries) {
            val sleepTime = retryInterval * retried;
            logger.warn(s"will retry to send after ${sleepTime}ms");
            Thread.sleep(sleepTime);
          } else {
            throw e;
          }
        case Success(_) =>
      }
    }
  }
}
