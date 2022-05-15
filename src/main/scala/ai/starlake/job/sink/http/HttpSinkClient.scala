package ai.starlake.job.sink.http

import com.typesafe.scalalogging.StrictLogging
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

private[http] class HttpSinkClient(url: String, maxMessages: Int, transformer: SinkTransformer)
    extends StrictLogging {

  val connectionManager = new PoolingHttpClientConnectionManager
  connectionManager.setMaxTotal(200);
  connectionManager.setDefaultMaxPerRoute(20);

  private def client =
    HttpClients
      .custom()
      .setConnectionManager(connectionManager)
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
      val response = client.execute(requestUri)
      val ok = 200 to 299 contains response.getStatusLine.getStatusCode
      if (!ok)
        throw new RuntimeException(response.getStatusLine.getReasonPhrase)

      val responseBody = EntityUtils.toString(response.getEntity, "UTF-8")
      logger.debug("Response from HTTP Sink: " + responseBody)
      response.getStatusLine.getStatusCode
    }
  }

}
