package ai.starlake.job.sink.pubsub

import better.files.File
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{
  AcknowledgeRequest,
  ProjectSubscriptionName,
  ProjectTopicName,
  PubsubMessage,
  PullRequest
}
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{FileInputStream, IOException}
import scala.jdk.CollectionConverters.{IterableHasAsJava, ListHasAsScala}

class PubSubProviderTest extends AnyFlatSpec with Matchers with StrictLogging {

  // GCP authentication is made using the GOOGLE_APPLICATION_CREDENTIALS environment variable
  private val projectId = "phenytech-global"
  private val subscriptionId = "ity-prd-sub-mycar"
  private val topicId = "ity-prd-top-mycar"
  private val message =
    "{\"myCars\":[{\"color\":\"red\",\"id\":\"1\", \"brand\":\"mercedes\"},{\"color\":\"blue\",\"id\":\"2\", \"brand\":\"bmw\"},{\"color\":\"blue\",\"id\":\"3\", \"brand\":\"porsche\"}]}"

  s"Save in Pubsub Sink" should "work" in {
    val spark = SparkSession.builder
      .master("local[1]")
      .getOrCreate();
    File("/tmp/sink").delete(true)
    File("/tmp/sink").createDirectory()
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/sink");

    val sqlContext = spark.sqlContext;
    // reads data from memory
    import spark.implicits._

    var outputStream = ""

    val events = new MemoryStream[String](1, sqlContext)
    val streamingQuery = events
      .toDF()
      .writeStream
      .format("pubsub")
      .option("gcpProjectId", projectId)
      .option("gcpTopicId", topicId)
      .start
    events.addData(message)
    // streamingQuery.processAllAvailable()
    streamingQuery.awaitTermination(8000)

    // Starting subscriber to receive messages
    val subscriberStubSettings: SubscriberStubSettings = SubscriberStubSettings.newBuilder
      .setTransportChannelProvider(
        SubscriberStubSettings.defaultGrpcTransportProviderBuilder
          .setMaxInboundMessageSize(20 * 1024 * 1024)
          .build
      )
      .build
    val subscriber = GrpcSubscriberStub.create(subscriberStubSettings)
    try {
      val subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId)
      val pullRequest =
        PullRequest.newBuilder.setMaxMessages(100).setSubscription(subscriptionName).build
      val pullResponse = subscriber.pullCallable.call(pullRequest)
      // Stop the program if the pull response is empty to avoid acknowledging
      // an empty list of ack IDs.
      if (pullResponse.getReceivedMessagesList.isEmpty) {
        logger.info("No message was pulled. Exiting.")
      }

      logger.info(
        s"Received messages count : ${pullResponse.getReceivedMessagesList.asScala.length}"
      )
      val ackIds = pullResponse.getReceivedMessagesList.asScala.map { message =>
        outputStream = message.getMessage.getData.toStringUtf8
        message.getAckId
      }
      // Acknowledge received messages.
      val acknowledgeRequest = AcknowledgeRequest.newBuilder
        .setSubscription(subscriptionName)
        .addAllAckIds(ackIds.asJava)
        .build
      // Use acknowledgeCallable().futureCall to asynchronously perform this operation.
      subscriber.acknowledgeCallable.call(acknowledgeRequest)
      subscriber
    } finally if (subscriber != null) subscriber.close()

    logger.info(s"outputStream: $outputStream")
    logger.info(s"message: $message")
    outputStream should be(message)
  }
  s"Load from PUBSUB Source to console" should "work" in {
    val spark = SparkSession.builder
      .master("local[1]")
      .getOrCreate();

    // reads data from pubsub
    val df = spark.readStream
      .format("pubsub")
      .option("gcpProjectId", projectId)
      .option("gcpSubscriptionId", subscriptionId)
      .load()

    // Create the topic name
    val topicName = ProjectTopicName.of(projectId, topicId)

    // Initialize the publisher
    val publisher = Publisher.newBuilder(topicName).build()

    try {
      // Create the message
      val data = ByteString.copyFromUtf8(message)
      val pubsubMessage = PubsubMessage.newBuilder().setData(data).build()

      // Publish the message
      val messageIdFuture = publisher.publish(pubsubMessage)
      val messageId = messageIdFuture.get()
      println(s"Published message ID: $messageId")
    } finally {
      // Shutdown the publisher
      if (publisher != null) {
        publisher.shutdown()
      }
    }

    df.writeStream
      .format("memory")
      .queryName("pubsubdata")
      .outputMode("append")
      .start()
      .awaitTermination(8000)
    val pubsubData = spark
      .sql("select value from pubsubdata")
      .collect()
      .map(_.getAs[String](0))
    logger.info(s"pubsubData.toList: ${pubsubData.toList}")
    pubsubData.toList should contain theSameElementsAs List(message)
  }

}
