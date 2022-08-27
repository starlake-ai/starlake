package ai.starlake.job.sink.http

import ai.starlake.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

import scala.util.{Failure, Success, Try}

class HtttpSink(
  url: String,
  maxMessages: Int,
  numRetries: Int,
  retryInterval: Int,
  transformer: SinkTransformer
) extends Sink
    with StrictLogging {
  val client = new HttpSinkClient(url, maxMessages, transformer)

  override def addBatch(batchId: Long, data: DataFrame) {
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
