package ai.starlake.job.sink.http

import ai.starlake.utils.Utils
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.client.methods.{HttpPost, HttpUriRequest}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}

trait SinkTransformer {
  def requestUris(url: String, rows: Array[Seq[String]]): Seq[HttpUriRequest]
}

object DefaultSinkTransformer extends SinkTransformer {
  val mapper: ObjectMapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(Include.NON_EMPTY)

  def requestUris(url: String, rows: Array[Seq[String]]): Seq[HttpUriRequest] =
    rows.map { row =>
      val jsonValue = mapper.writeValueAsString(row)
      val requestEntity =
        new StringEntity(jsonValue, ContentType.APPLICATION_JSON);
      val httpPost = new HttpPost(url)
      httpPost.setEntity(requestEntity)
      httpPost
    }
}

class HttpSinkProvider extends StreamSinkProvider with DataSourceRegister {
  def createSink(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode
  ): HtttpSink = {
    val transformer = parameters
      .get("transformer")
      .map(Utils.loadInstance[SinkTransformer])
      .getOrElse(DefaultSinkTransformer)

    val url = parameters("url")
    val maxMessages = parameters.getOrElse("maxMessages", "1").toInt
    val numRetries = parameters.getOrElse("numRetries", "3").toInt
    val retryInterval = parameters.getOrElse("retryInterval", "1000").toInt
    new HtttpSink(url, maxMessages, numRetries, retryInterval, transformer);
  }

  def shortName(): String = "starlake-http"
}

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
