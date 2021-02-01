package com.ebiznext.comet.job.index.kafkaload

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.kafka.KafkaClient
import com.ebiznext.comet.utils.{JobResult, SparkJob, SparkJobResult, Utils}
import org.apache.spark.sql.DataFrame

import scala.util.Try

class KafkaJob(
  val kafkaJobConfig: KafkaJobConfig
)(implicit val settings: Settings)
    extends SparkJob {

  private val topicConfig: Settings.KafkaTopicOptions =
    settings.comet.kafka.topics(kafkaJobConfig.topic)

  def offload(): Try[SparkJobResult] = {
    Try {
      Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaUtils =>
        val (df, offsets) = kafkaUtils
          .consumeTopic(
            kafkaJobConfig.topic,
            session,
            topicConfig
          )

        val transformedDF = transfom(df)

        val res = kafkaJobConfig.path match {
          case Some(path) =>
            transformedDF.write
              .mode(kafkaJobConfig.mode)
              .format(kafkaJobConfig.format)
              .save(path)
            kafkaUtils.topicSaveOffsets(
              kafkaJobConfig.topic,
              topicConfig.accessOptions,
              offsets
            )
            transformedDF
          case None =>
            logger.warn("Offsets not updated when no sink provided")
            transformedDF
        }
        SparkJobResult(Some(res))
      }
    }
  }

  def load(): Try[SparkJobResult] = {
    Try {
      Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaUtils =>
        kafkaJobConfig.path match {
          case Some(path) =>
            val df = session.read.format(kafkaJobConfig.format).load(path.split(','): _*)
            val transformedDF = transfom(df)

            kafkaUtils.sinkToTopic(
              kafkaJobConfig.topic,
              topicConfig,
              transformedDF
            )
            SparkJobResult(Some(transformedDF))
          case None =>
            throw new Exception("No path provided !")
        }
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

  override def name: String = s"${kafkaJobConfig.topic}"
}
