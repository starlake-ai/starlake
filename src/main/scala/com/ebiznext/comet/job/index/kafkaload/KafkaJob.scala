package com.ebiznext.comet.job.index.kafkaload

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.kafka.KafkaTopicUtils
import com.ebiznext.comet.utils.{JobResult, SparkJob, SparkJobResult}
import org.apache.spark.sql.SaveMode

import scala.util.Try

class KafkaJob(
  val kafkaJobConfig: KafkaJobConfig
)(implicit val settings: Settings)
    extends SparkJob {

  def offload(): Try[SparkJobResult] = {
    Try {
      val kafkaUtils = new KafkaTopicUtils(settings.comet.kafka.serverOptions)
      val df = kafkaUtils
        .consumeTopic(
          kafkaJobConfig.topic,
          session,
          settings.comet.kafka.topics(kafkaJobConfig.topic)
        )
      val res = kafkaJobConfig.input match {
        case Some(Left(path)) =>
          df.write
            .mode(kafkaJobConfig.mode)
            .format(kafkaJobConfig.format)
            .save(path)
          df
        case Some(Right(outDF)) =>
          kafkaJobConfig.mode match {
            case SaveMode.Overwrite =>
              df
            case SaveMode.Append =>
              df union outDF
            case SaveMode.Ignore =>
              outDF
            case SaveMode.ErrorIfExists =>
              throw new Exception("Option ErorIfExists not applicable here")
          }
        case None =>
          df
      }
      SparkJobResult(Some(res))
    }
  }

  def load(): Try[SparkJobResult] = {
    val kafkaUtils = new KafkaTopicUtils(settings.comet.kafka.serverOptions)
    Try {
      kafkaJobConfig.input match {
        case Some(input) =>
          val df = input match {
            case Left(path) =>
              session.read.format(kafkaJobConfig.format).load(path.split(','): _*)
            case Right(inputDF) =>
              inputDF
          }
          kafkaUtils.sinkToTopic(
            kafkaJobConfig.topic,
            settings.comet.kafka.topics(kafkaJobConfig.topic),
            df
          )
          SparkJobResult(Some(df))
        case None =>
          throw new Exception("No input provided !")
      }
    }
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
