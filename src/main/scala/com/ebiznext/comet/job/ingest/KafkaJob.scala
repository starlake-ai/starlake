package com.ebiznext.comet.job.ingest

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.kafka.TopicAdmin
import com.ebiznext.comet.utils.{JobResult, SparkJob}
import org.apache.kafka.clients.admin.NewTopic

import scala.util.Try

class KafkaJob(
  val topic: String
)(implicit val settings: Settings)
    extends SparkJob {
  val EARLIEST_OFFSET = -2L

  val topicAdmin = new TopicAdmin(settings.comet.kafka.servers("server1"))

  override def name: String = s"""${topic}"""

  override def run(): Try[JobResult] = {
    import session.implicits._
    val topicOptions = settings.comet.kafka.topics(topic).options
    val reader = session.read
      .format("kafka")
    val startOffsets =
      topicCurrentOffsets(topic)
        .getOrElse {
          topicAdmin.topicPartitions(topic).map(p => (p.partition(), EARLIEST_OFFSET))
        }
    val endOffsets = topicAdmin.topicEndOffsets(topic)

    val withOffsetsTopicOptions = topicOptions ++ Seq(
      "startingOffsets" -> offsetsAsJson(topic, startOffsets),
      "endingOffsets"   -> offsetsAsJson(topic, endOffsets)
    )

    // TODO Loop based on maxRead need to be implemented here

    val df =
      withOffsetsTopicOptions
        .foldLeft(reader)((reader, option) => reader.option(option._1, option._2))
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    ???
  }

  def offsetsAsJson(topicName: String, offsets: List[(Int, Long)]) = {
    val offsetsAsString = offsets.map { offset =>
      s""""${offset._1}": ${offset._2}"""
    } mkString ","
    s"""{"$topicName":{$offsetsAsString}}"""
  }

  def topicCurrentOffsets(topicName: String): Option[List[(Int, Long)]] = {
    import session.implicits._

    val cometOffsetOptions = settings.comet.kafka.topics("comet_offsets")
    val cometOffsetTopic = new NewTopic(
      "comet_offsets",
      cometOffsetOptions.partitions,
      cometOffsetOptions.replicationFactor
    )
    topicAdmin.createTopicIfNotPresent(cometOffsetTopic, cometOffsetOptions.options)

    val reader = session.read.format("kafka")

    val df = cometOffsetOptions.options
      .foldLeft(reader)((reader, option) => reader.option(option._1, option._2))
      .load()

    val offsets: Map[String, String] = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .collect()
      .toMap
    offsets.keys.map { k =>
      val tab = k.split('/')
      (tab(0), tab(1), offsets(k))
    } groupBy (_._1) mapValues (_.map(it => (it._2.toInt, it._3.toLong)).toList) get topicName
  }

}
