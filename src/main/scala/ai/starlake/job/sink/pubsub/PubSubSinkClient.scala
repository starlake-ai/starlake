package ai.starlake.job.sink.pubsub

import ai.starlake.job.sink.http.{DefaultSinkTransformer, SinkTransformer}
import ai.starlake.utils.Utils
import com.google.api.core.{ApiFutureCallback, ApiFutures}
import com.google.api.gax.rpc.ApiException
import com.google.cloud.pubsub.v1.Publisher
import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, TopicName}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame

import java.io.IOException
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

private[pubsub] class PubSubSinkClient(parameters: Map[String, String]) extends StrictLogging {

  val maxMessages: Int = parameters.getOrElse("maxMessages", "1").toInt
  val maxTotalThreads: Int = parameters.getOrElse("maxTotalThreads", "200").toInt
  val maxThreadsPerRoute: Int = parameters.getOrElse("maxThreadsPerRoute", "20").toInt
  val maxIdleTimeInSeconds: Int = parameters.getOrElse("maxIdleTimeInSeconds", "10").toInt
  val transformer: SinkTransformer = parameters
    .get("transformer")
    .map(Utils.loadInstance[SinkTransformer])
    .getOrElse(DefaultSinkTransformer)

  private val projectId = parameters.getOrElse("gcpProjectId", "not_found_in_parameters")
  private val topicId = parameters.getOrElse("gcpTopicId", "not_found_in_parameters")
  assert(
    projectId != "not_found_in_parameters",
    "Parameter gcpProjectId is mandatory"
  )
  assert(
    topicId != "not_found_in_parameters",
    "Parameter gcpTopicId is mandatory"
  )

  private def client: Publisher = {
    val topicName = TopicName.of(projectId, topicId)
    Publisher.newBuilder(topicName).build
  }

  def send(dataFrame: DataFrame): Int = {
    // performed on the driver node instead of worker nodes, so use local iterator
    val iter = dataFrame.toLocalIterator()
    val buffer = ArrayBuffer[Seq[String]]()
    var count = 0
    while (iter.hasNext) {
      val row = iter.next().toSeq.map(Option(_).getOrElse("").toString) // deal with null values
      val transformed = row
      buffer += transformed
      count = count + 1
      if (count % maxMessages == 0) {
        publishWithErrorHandlerExample(client, buffer.flatten.toArray)
        buffer.clear()
      }
    }
    if (buffer.nonEmpty)
      publishWithErrorHandlerExample(client, buffer.flatten.toArray)
    count
  }

  @throws[IOException]
  @throws[InterruptedException]
  private def publishWithErrorHandlerExample(
    publisher: Publisher,
    messages: Array[String]
  ): Unit = {
    val topicName = TopicName.of(projectId, topicId)
    // var publisher: Publisher = null
    try {
      // Create a publisher instance with default settings bound to the topic
      for (message <- messages) {
        val data = ByteString.copyFromUtf8(message)
        val pubsubMessage = PubsubMessage.newBuilder.setData(data).build
        // Once published, returns a server-assigned message id (unique within the topic)
        val future = publisher.publish(pubsubMessage)
        // Add an asynchronous callback to handle success / failure
        ApiFutures.addCallback(
          future,
          new ApiFutureCallback[String]() {
            override def onFailure(throwable: Throwable): Unit = {
              if (throwable.isInstanceOf[ApiException]) {
                val apiException = throwable.asInstanceOf[ApiException]
                // details on the API exception
                logger.info(apiException.getStatusCode.getCode.toString)
                logger.info(apiException.getReason)
                logger.info(apiException.getMessage)
                logger.info(apiException.isRetryable.toString)
                logger.info(pubsubMessage.toString)
              }
              logger.info("Error publishing message : " + message)
            }

            override def onSuccess(messageId: String): Unit = {
              // Once published, returns server-assigned message ids (unique within the topic)
              logger.info("Published message ID: " + messageId)
            }
          },
          MoreExecutors.directExecutor
        )
      }
    } finally
      if (publisher != null) {
        // When finished with the publisher, shutdown to free up resources.
        publisher.shutdown()
        publisher.awaitTermination(1, TimeUnit.MINUTES)
      }
  }

}
