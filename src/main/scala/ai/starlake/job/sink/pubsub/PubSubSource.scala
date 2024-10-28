package ai.starlake.job.sink.pubsub

import ai.starlake.job.sink.DataFrameTransform
import ai.starlake.utils.Utils
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.sun.net.httpserver.HttpExchange
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.pubsub.v1.{
  AcknowledgeRequest,
  ProjectSubscriptionName,
  PubsubMessage,
  PullRequest
}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.ai.starlake.http.HttpSourceProxy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, SerializedOffset, Source}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String
import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.TimeoutException
import scala.jdk.CollectionConverters.{IterableHasAsJava, ListHasAsScala}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer

case class PubSubPayload(data: String)
class PubSubSource(sqlContext: SQLContext, parameters: Map[String, String])
    extends HttpSourceProxy
    with Source
    with StrictLogging {

  private val projectId = parameters.getOrElse("gcpProjectId", "not_found_in_parameters")
  private val subscriptionId = parameters.getOrElse("gcpSubscriptionId", "not_found_in_parameters")
  assert(
    projectId != "not_found_in_parameters",
    s"Parameter gcpProjectId is mandatory : ${parameters}"
  )
  assert(
    subscriptionId != "not_found_in_parameters",
    "Parameter gcpSubscriptionId is mandatory"
  )
  private val awaitTimeout = sys.env.get("AWAIT_TIMEOUT").map(_.toInt).getOrElse(600)
  private val numOfMessages = sys.env.get("NUM_OF_MESSAGES").map(_.toInt).getOrElse(1000)

  override def schema: StructType = StructType(List(StructField("value", StringType, true)))
  private var producerOffset: LongOffset = new LongOffset(-1);
  private var consumerOffset = -1;
  private val streamBuffer = ListBuffer.empty[PubSubPayload]

  /** Send back a response with provided status code and response text */
  private def sendResponse(he: HttpExchange, status: Int, response: String): Unit = {
    he.sendResponseHeaders(status, response.length)
    val os = he.getResponseBody()
    os.write(response.getBytes)
    os.close()
  }

  def processMessages(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
    // do the thing
    val payload = message.getData.toStringUtf8
    logger.info(s"Processing message : ${payload}")
    producerOffset += 1;
    streamBuffer += PubSubPayload(payload)
  }

  def callEndPoint(data: String): Unit = {
    logger.info(s"Calling endpoint with data : ${data}")
  }

  /** this thread move data from HttpTextReceiver to local buffer periodically create server */
  private def startSubscriberSync(): GrpcSubscriberStub = {
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
        PullRequest.newBuilder.setMaxMessages(numOfMessages).setSubscription(subscriptionName).build
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
        processMessages(message.getMessage, null)
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
  }

  private def startSubscriberAsync(): Subscriber = {
    logger.info("ProjectSubscriptionName...")
    val subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId)

    // Instantiate an asynchronous message receiver.
    logger.info("defining receiver...")
    val receiver: MessageReceiver = (message: PubsubMessage, consumer: AckReplyConsumer) => {
      // Handle incoming message, then ack the received message.
      logger.info(s"Processing Message Id: ${message.getMessageId}")
      processMessages(message, consumer)
      consumer.ack
      logger.info(s"Message ${message.getMessageId} acknowledged.")
    }

    var subscriber: Subscriber = null
    try {
      logger.info("Subscriber.newBuilder...")
      subscriber = Subscriber.newBuilder(subscriptionName, receiver).build()
      // Start the subscriber.
      logger.info("Starting subscriber async...")
      subscriber.startAsync.awaitRunning()
      // .awaitRunning
      logger.info(s"Listening for messages on ${subscriptionName.toString}")
      // Allow the subscriber to run for Xs unless an unrecoverable error occurs.
      subscriber.awaitTerminated()
    } catch {
      case timeoutException: TimeoutException =>
        // Shut down the subscriber after 30s. Stop receiving messages.
        logger.info(
          s"Shut down the subscriber after ${awaitTimeout.toString}s. Stop receiving messages."
        )
        subscriber.stopAsync
    }
    subscriber
  }

  logger.info(s"Starting PubSub subscriber...")

  // Encapsuler la méthode startSubscriberAsync dans un Runnable
  val subscriberRunnable = new Runnable {
    override def run(): Unit = {
      startSubscriberAsync()
    }
  }

  // Créer et démarrer un nouveau Thread avec ce Runnable
  private val subscriberThread: Thread = new Thread(subscriberRunnable)
  subscriberThread.start()

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
  def convertToLongOffset(offset: Offset): Option[LongOffset] = {
    logger.info("PubSubSource::convertToLongOffset : in function")
    offset match {
      case lo: LongOffset       => Some(lo)
      case so: SerializedOffset => Some(LongOffset(so))
      case _                    => None
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logger.info("PubSubSource::getBatch : in function")
    val iStart =
      convertToLongOffset(start.getOrElse(LongOffset(-1))).getOrElse(LongOffset(-1)).offset;
    val iEnd = convertToLongOffset(end).getOrElse(LongOffset(-1)).offset;
    val slices = this.synchronized {
      val messages = streamBuffer.slice(iStart.toInt - consumerOffset, iEnd.toInt - consumerOffset)
      messages
    }
    val dfs = slices.map { case (slice) =>
      val rdd: RDD[InternalRow] = sqlContext.sparkContext.parallelize(Seq(slice)).map { item =>
        InternalRow(UTF8String.fromString(item.data))
      }
      val dataframe = internalCreateDataFrame(
        sqlContext.sparkSession,
        rdd,
        StructType(List(StructField("value", StringType))),
        isStreaming = true
      )
      DataFrameTransform.transform(
        Some(Utils.loadInstance("ai.starlake.job.sink.IdentityDataFrameTransformer")),
        dataframe,
        sqlContext.sparkSession
      )
    }.toList
    logger.info(s"dfs.size : ${dfs.size}")
    if (dfs.size == 1)
      dfs.head
    else
      dfs.reduce(_ union _)
  }

  override def commit(end: Offset): Unit = {
    // discards [0, end] lines, since they have been consumed
    logger.info("PubSubSource::commit : in function")
    val optEnd = convertToLongOffset(end);
    optEnd match {
      case Some(LongOffset(iOffset: Long)) =>
        if (iOffset >= 0) {
          this.synchronized {
            streamBuffer.trimStart(iOffset.toInt - consumerOffset);
            consumerOffset = iOffset.toInt;
          }
        }
      case _ => throw new Exception(s"Cannot commit with end offset => $end");
    }
  }

  override def stop(): Unit = {
    logger.info("PubSubSource::stop : in function")
    // server.close()
    subscriberThread.interrupt()
  }
}
