package ai.starlake.job.sink.http

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DatasetLogging, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.InetSocketAddress

class HttpSinkProviderTest
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with StrictLogging
    with DatasetLogging {

  def startHttpServer(): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress(9000), 0)
    server.createContext("/", new RootHandler)
    server.setExecutor(null)
    server.start()
    server
  }
  val outputStream = new ByteArrayOutputStream

  class RootHandler extends HttpHandler {
    def handle(t: HttpExchange) {
      logPayload(t.getRequestBody)
      sendResponse(t)
    }
    private def logPayload(body: InputStream): Unit = {
      Iterator
        .continually(body.read)
        .takeWhile(-1 != _)
        .foreach(outputStream.write)
    }

    private def sendResponse(t: HttpExchange) {
      val response = "Ack!"
      t.sendResponseHeaders(200, response.length())
      val os = t.getResponseBody
      os.write(response.getBytes)
      os.close()
    }
  }
  s"Save in HTTP Sink" should "work" in {
    val spark = SparkSession.builder
      .master("local[4]")
      .getOrCreate();
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

    val sqlContext = spark.sqlContext;
    // reads data from memory
    import spark.implicits._

    val server = startHttpServer()
    val events = new MemoryStream[String](1, sqlContext)
    val streamingQuery = events
      .toDF()
      .writeStream
      .format("starlake-http")
      .option("url", "http://localhost:9000")
      .start
    events.addData("0", "1", "2")
    // streamingQuery.processAllAvailable()
    streamingQuery.awaitTermination(2000)
    server.stop(0)

    outputStream.toString should be("""["0"]["1"]["2"]""")

  }
}
