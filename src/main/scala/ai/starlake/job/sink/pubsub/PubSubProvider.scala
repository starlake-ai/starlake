package ai.starlake.job.sink.pubsub

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.util.UUID

class PubSubProvider extends StreamSinkProvider with StreamSourceProvider with DataSourceRegister {
  def createSink(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode
  ): PubSubSink = {
    new PubSubSink(parameters);
  }

  def shortName(): String = "pubsub"

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
    new PubSubSource(sqlContext, parameters)

}
