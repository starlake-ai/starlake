package ai.starlake.job.sink.http

import better.files.File
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DatasetLogging, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.InetSocketAddress

class HttpProviderTest
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with StrictLogging
    with DatasetLogging {

  def startHttpServer(): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress(9000), 0)
    server.createContext("/", new RootHandler())
    server.setExecutor(null)
    server.start()
    server
  }
  val outputStream = new ByteArrayOutputStream()

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

  s"Load from HTTP Source to multiple URLs" should "work" in {
    val spark = SparkSession.builder
      .master("local[4]")
      .getOrCreate();
    File("/tmp/http2").delete(true)
    spark.conf.set("spark.sql.streaming.checkpointLocation", s"/tmp/http2");

    val sqlContext = spark.sqlContext;
    // reads data from memory

    val df = spark.readStream
      .format("starlake-http")
      .option("port", "10000")
      .option("urls", "/test1|/test2")
      .option(
        "transformers",
        "ai.starlake.job.sink.IdentityDataFrameTransformer|ai.starlake.job.sink.IdentityDataFrameTransformer"
      )
      .load()
    val thread = new Thread {
      override def run {
        Thread.sleep(2000)
        val post1 = new HttpPost("http://localhost:10000/test1")
        val post2 = new HttpPost("http://localhost:10000/test2")
        val client = HttpClientBuilder.create.build()
        post1.setEntity(new StringEntity("http data1"))
        client.execute(post1)
        post2.setEntity(new StringEntity("http data2"))
        client.execute(post2)
        client.close()
      }
    }
    thread.start()
    // df.writeStream.format("console").start().awaitTermination(30000)
    df.writeStream
      .format("memory")
      .queryName("http")
      .outputMode("append")
      .start()
      .awaitTermination(10000)
    val httpData = spark
      .sql("select value from http")
      .collect()
      .map(_.getAs[String](0))
    httpData.toList should contain theSameElementsAs List("http data1", "http data2")
  }
  s"Save in HTTP Sink" should "work" in {
    val spark = SparkSession.builder
      .master("local[4]")
      .getOrCreate();
    File("/tmp/sink").delete(true)
    File("/tmp/sink").createDirectory()
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/sink");

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
