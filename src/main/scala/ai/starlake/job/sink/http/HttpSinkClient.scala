package ai.starlake.job.sink.http

import ai.starlake.utils.Utils
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.DataFrame

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

private[http] class HttpSinkClient(parameters: Map[String, String]) extends StrictLogging {

  val url: String = parameters("url")
  val maxMessages: Int = parameters.getOrElse("maxMessages", "1").toInt
  val maxTotalThreads: Int = parameters.getOrElse("maxTotalThreads", "200").toInt
  val maxThreadsPerRoute: Int = parameters.getOrElse("maxThreadsPerRoute", "20").toInt
  val maxIdleTimeInSeconds: Int = parameters.getOrElse("maxIdleTimeInSeconds", "10").toInt
  val transformer: SinkTransformer = parameters
    .get("transformer")
    .map(Utils.loadInstance[SinkTransformer])
    .getOrElse(DefaultSinkTransformer)

  val connectionManager = new PoolingHttpClientConnectionManager()
  connectionManager.setMaxTotal(maxTotalThreads);
  connectionManager.setDefaultMaxPerRoute(maxThreadsPerRoute);

  private def client =
    HttpClients
      .custom()
      .setConnectionManager(connectionManager)
      .evictExpiredConnections()
      .evictIdleConnections(maxIdleTimeInSeconds, TimeUnit.SECONDS)
      .build();

  def send(dataFrame: DataFrame): Int = {
    // performed on the driver node instead of worker nodes, so use local iterator
    val iter = dataFrame.toLocalIterator;
    val buffer = ArrayBuffer[Seq[String]]();
    var count = 0
    while (iter.hasNext) {
      val row = iter.next().toSeq.map(Option(_).getOrElse("").toString) // deal with null values
      val transformed = row
      buffer += transformed
      count = count + 1
      if (count % maxMessages == 0) {
        post(buffer.toArray);
        buffer.clear()
      }
    }
    if (buffer.nonEmpty)
      post(buffer.toArray);
    count
  }

  private def post(rows: Array[Seq[String]]): Unit = {
    logger.debug(s"request: ${rows.mkString("Array(", ", ", ")")}");
    transformer.requestUris(url, rows).foreach { requestUri =>
      Utils.withResources(client.execute(requestUri)) { response =>
        try {
          val ok = (200 to 299) contains response.getStatusLine.getStatusCode
          if (!ok)
            throw new RuntimeException(response.getStatusLine.getReasonPhrase)

          val responseBody = EntityUtils.toString(response.getEntity, "UTF-8")
          logger.debug("Response from HTTP Sink: " + responseBody)
          response.getStatusLine.getStatusCode
        } finally {
          response.close()
        }
      }
    }
  }

}
