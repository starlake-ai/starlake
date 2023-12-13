package ai.starlake.job.sink.kafka

import ai.starlake.utils.Utils
import org.apache.spark.sql.SaveMode

/** Kafka 2 Kafka: Source Configuration kafka connection properties Topic properties (fill
  * deserializers and topic name) Sink Configuration Kafka connection properties Topic properties
  * (fill serializers and topic name) Transformers transformer Mode Batch or streaming Streaming
  * properties
  */

case class KafkaJobConfig(
  connectionRef: Option[String] = None,
  topicConfigName: Option[String] = None,
  format: String = "parquet",
  path: Option[String] = None,
  options: Map[String, String] = Map.empty,
  transform: Option[String] = None,
  writeTopicConfigName: Option[String] = None,
  writeOptions: Map[String, String] = Map.empty,
  writeMode: String = SaveMode.Append.toString,
  writeFormat: String = "kafka",
  writePath: Option[String] = None,
  coalesce: Option[Int] = None,
  streaming: Boolean = false,
  streamingTrigger: Option[String] = Some("Once"), // Once, ProcessingTime, Continuous or None
  streamingTriggerOption: String = "10 seconds",
  streamingWritePartitionBy: Seq[String] = Nil,
  streamingWriteToTable: Boolean = false
) {
  override def toString: String = {
    s"""
       |KafkaJobConfig(
       |  connectionRef: $connectionRef,
       |  topicConfigName: $topicConfigName,
       |  format: $format,
       |  path: $path,
       |  options: ${Utils.redact(options)},
       |  transform: $transform,
       |  writeTopicConfigName: $writeTopicConfigName,
       |  writeOptions: ${Utils.redact(writeOptions)},
       |  writeMode: $writeMode,
       |  writeFormat: $writeFormat,
       |  writePath: $writePath,
       |  coalesce: $coalesce,
       |  streaming: $streaming,
       |  streamingTrigger: $streamingTrigger,
       |  streamingTriggerOption: $streamingTriggerOption,
       |  streamingWritePartitionBy: $streamingWritePartitionBy,
       |  streamingWriteToTable: $streamingWriteToTable
       |)
       |""".stripMargin
  }
}
