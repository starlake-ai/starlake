package ai.starlake.job.sink.http

import ai.starlake.job.sink.DataFrameTransform
import ai.starlake.utils.Utils
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.ai.starlake.http.HttpSourceProxy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, SerializedOffset, Source}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.net.InetSocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer

// https://github.com/bluejoe2008/spark-http-stream/blob/master/src/main/scala/org/apache/spark/sql/execution/streaming/http/ActionsHandler.scala
//  https://github.com/hienluu/wikiedit-streaming/blob/master/streaming-receiver/src/main/scala/org/twitterstreaming/receiver/TwitterStreamingSource.scala

class HttpSource(sqlContext: SQLContext, port: Int, transfomer: Option[String])
    extends HttpSourceProxy
    with Source
    with StrictLogging {

  override def schema: StructType = StructType(List(StructField("value", StringType, true)))
  private var producerOffset: LongOffset = new LongOffset(-1);
  private var consumerOffset = -1;
  private val streamBuffer = ListBuffer[String]();
  val flagStop = new AtomicBoolean(false);

  /** Send back a response with provided status code and response text */
  private def sendResponse(he: HttpExchange, status: Int, response: String): Unit = {
    he.sendResponseHeaders(status, response.length)
    val os = he.getResponseBody()
    os.write(response.getBytes)
    os.close()
  }

  /** this thread move data from HttpTextReceiver to local buffer periodically create server */
  private def startServer(): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress(port), 0)
    server.setExecutor(Executors.newCachedThreadPool())
    server.createContext(
      "/",
      new HttpHandler {
        override def handle(httpExchange: HttpExchange): Unit = {
          val payload = scala.io.Source.fromInputStream(httpExchange.getRequestBody).mkString
          producerOffset += 1;
          streamBuffer += payload;
          sendResponse(httpExchange, status = 200, response = """{"success": true}""")
        }
      }
    )
    // Start server and return streaming DF
    server.start()
    server
  }

  logger.info(s"Http Server started on port $port")
  private val server: HttpServer = startServer()

  override def getOffset: Option[Offset] = {
    val po = producerOffset;
    if (po.offset == -1) {
      None
    } else {
      Some(po)
    }
  }

  /** Convert generic Offset to LongOffset if possible.
    * @return
    *   converted LongOffset
    */
  def convertToLongOffset(offset: Offset): Option[LongOffset] = offset match {
    case lo: LongOffset       => Some(lo)
    case so: SerializedOffset => Some(LongOffset(so))
    case _                    => None
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val iStart =
      convertToLongOffset(start.getOrElse(LongOffset(-1))).getOrElse(LongOffset(-1)).offset;
    val iEnd = convertToLongOffset(end).getOrElse(LongOffset(-1)).offset;
    val slice = this.synchronized {
      streamBuffer.slice(iStart.toInt - consumerOffset, iEnd.toInt - consumerOffset);
    }
    val rdd: RDD[InternalRow] = sqlContext.sparkContext.parallelize(slice).map { item =>
      InternalRow(UTF8String.fromString(item))
    }
    val dataframe = internalCreateDataFrame(
      sqlContext.sparkSession,
      rdd,
      StructType(List(StructField("value", StringType))),
      isStreaming = true
    )
    DataFrameTransform.transform(transformInstance, dataframe, sqlContext.sparkSession)
  }

  private val transformInstance: Option[DataFrameTransform] =
    transfomer.map(Utils.loadInstance[DataFrameTransform])

  override def commit(end: Offset) {
    // discards [0, end] lines, since they have been consumed
    val optEnd = convertToLongOffset(end);
    optEnd match {
      case Some(LongOffset(iOffset: Long)) ⇒
        if (iOffset >= 0) {
          this.synchronized {
            streamBuffer.trimStart(iOffset.toInt - consumerOffset);
            consumerOffset = iOffset.toInt;
          }
        }
      case _ ⇒ throw new Exception(s"Cannot commit with end offset => $end");
    }
  }

  override def stop() {
    server.stop(30)
  }
}
