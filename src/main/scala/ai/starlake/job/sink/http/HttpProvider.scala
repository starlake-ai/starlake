package ai.starlake.job.sink.http

import ai.starlake.utils.Utils
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.http.client.methods.{HttpPost, HttpUriRequest}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.util.UUID

trait SinkTransformer {
  def requestUris(url: String, rows: Array[Seq[String]]): Seq[HttpUriRequest]
}

object DefaultSinkTransformer extends SinkTransformer {
  val mapper: ObjectMapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.setSerializationInclusion(Include.NON_EMPTY)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

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

class HttpProvider extends StreamSinkProvider with StreamSourceProvider with DataSourceRegister {
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

  override def sourceSchema(
    sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]
  ): (String, StructType) = {
    (
      parameters.getOrElse("name", UUID.randomUUID().toString),
      StructType(List(StructField("value", StringType, true)))
    )
  }

  override def createSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]
  ): Source =
    new HttpSource(
      sqlContext,
      parameters("port").toInt,
      parameters.get("transformer")
    )

}
