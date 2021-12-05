package ai.starlake.job.index.kafkaload

import ai.starlake.config.Settings
import ai.starlake.utils.kafka.KafkaClient
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult, Utils}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

import scala.util.Try

class KafkaJob(
  val kafkaJobConfig: KafkaJobConfig
)(implicit val settings: Settings)
    extends SparkJob {

  private val topicConfig: Settings.KafkaTopicConfig =
    settings.comet.kafka.topics(kafkaJobConfig.topicConfigName)

  def offload(): Try[SparkJobResult] = {
    Try {
      if (!kafkaJobConfig.streaming) {
        Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
          val (df, offsets) = kafkaClient
            .consumeTopicBatch(
              kafkaJobConfig.topicConfigName,
              session,
              topicConfig
            )

          val transformedDF = transfom(df)
          val finalDF =
            kafkaJobConfig.coalesce match {
              case None    => transformedDF
              case Some(x) => transformedDF.coalesce(x)
            }

          logger.info(s"Saving to $kafkaJobConfig")
          finalDF.write
            .mode(kafkaJobConfig.mode)
            .format(kafkaJobConfig.format)
            .options(kafkaJobConfig.writeOptions)
            .save(kafkaJobConfig.path)
          logger.info(s"Kafka saved messages to offload -> ${kafkaJobConfig.path}")

          kafkaJobConfig.coalesce match {
            case Some(1) if kafkaJobConfig.coalesceMerge =>
              val extension = kafkaJobConfig.format
              val targetPath = new Path(kafkaJobConfig.path)
              val singleFile = settings.storageHandler
                .list(
                  targetPath,
                  recursive = false
                )
                .filter(_.getName.startsWith("part-"))
                .head
              val tmpPath = new Path(targetPath.toString + ".tmp")
              if (settings.storageHandler.move(singleFile, tmpPath)) {
                settings.storageHandler.delete(targetPath)
                settings.storageHandler.move(tmpPath, targetPath)
              }
            case _ =>
          }

          kafkaClient.topicSaveOffsets(
            kafkaJobConfig.topicConfigName,
            topicConfig.accessOptions,
            offsets
          )
          SparkJobResult(Some(transformedDF))
        }
      } else {
        Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
          val df = kafkaClient
            .consumeTopicStreaming(
              session,
              topicConfig
            )
          val transformedDF = transfom(df)
          val trigger = kafkaJobConfig.streamingTrigger.toLowerCase match {
            case "once"           => Trigger.Once()
            case "processingtime" => Trigger.ProcessingTime(kafkaJobConfig.streamingTriggerOption)
            case "continuous"     => Trigger.Continuous(kafkaJobConfig.streamingTriggerOption)
          }

          val writer = transformedDF.writeStream
            .outputMode(kafkaJobConfig.streamingWriteMode)
            .format(kafkaJobConfig.streamingWriteFormat)
            .options(kafkaJobConfig.writeOptions)
            .trigger(trigger)
          val partitionedWriter = kafkaJobConfig.streamingWritePartitionBy match {
            case Nil =>
              writer
            case list =>
              writer.partitionBy(list: _*)
          }
          val streamingQuery =
            if (kafkaJobConfig.streamingWriteToTable) {
              // partitionedWriter.toTable(kafkaJobConfig.path)
              throw new Exception("streamingWriteToTable Not Supported")
            } else
              partitionedWriter
                .start(kafkaJobConfig.path)

          streamingQuery
            .awaitTermination()
          SparkJobResult(None)
        }
      }
    }
  }

  def load(): Try[SparkJobResult] = {
    Try {
      Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
        val df = session.read.format(kafkaJobConfig.format).load(kafkaJobConfig.path.split(','): _*)
        val transformedDF = transfom(df)

        kafkaClient.sinkToTopic(
          topicConfig,
          transformedDF
        )
        SparkJobResult(Some(transformedDF))
      }
    }
  }

  private def transfom(df: DataFrame) = {
    val transformedDF = kafkaJobConfig.transformInstance match {
      case Some(transformer) =>
        transformer.transform(df)
      case None =>
        df
    }
    transformedDF
  }

  override def run(): Try[JobResult] = {
    if (kafkaJobConfig.offload) {
      offload()
    } else {
      load()
    }
  }

  override def name: String = s"${kafkaJobConfig.topicConfigName}"
}
