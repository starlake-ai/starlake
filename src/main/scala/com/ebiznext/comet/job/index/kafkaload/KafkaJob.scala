package com.ebiznext.comet.job.index.kafkaload

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.kafka.KafkaTopicUtils
import com.ebiznext.comet.utils.{JobResult, SparkJob, SparkJobResult}
import org.apache.spark.sql.SaveMode

import scala.util.Try

class KafkaJob(
  val loadConfig: KafkaJobConfig
)(implicit val settings: Settings)
    extends SparkJob {

  override def run(): Try[JobResult] = {
    Try {
      val kafkaUtils = new KafkaTopicUtils(settings.comet.kafka.serverOptions)
      if (loadConfig.offload) {
        val df = kafkaUtils
          .consumeTopic(loadConfig.topic, session, settings.comet.kafka.topics(loadConfig.topic))
        df.write
          .mode(SaveMode.Overwrite)
          .format(loadConfig.format)
          .save("/tmp/json")
        SparkJobResult(Some(df))
      } else {
        val df = session.read.format(loadConfig.format).load(loadConfig.path.split(','): _*)
        kafkaUtils.sinkToTopic(loadConfig.topic, settings.comet.kafka.topics(loadConfig.topic), df)
        SparkJobResult(Some(df))
      }
    }
  }

  override def name: String = s"${loadConfig.topic}"
}
