package com.ebiznext.comet.config

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkEnv(name: String)
  extends StrictLogging {

  val config = {
    val now = LocalDateTime
      .now()
      .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss.SSS"))
    val appName = s"$name-$now"
    val thisConf = new SparkConf().setAppName(appName)

    import scala.collection.JavaConversions._
    ConfigFactory.load().getConfig("spark")
      .entrySet()
      .map(x => (x.getKey, x.getValue.unwrapped().toString))
      .foreach {
        case (key, value) =>
          thisConf.set("spark." + key, value)
      }
    thisConf.set("spark.app.id", appName)
    thisConf.getAll.foreach(x => logger.info(x._1 + "=" + x._2))
    thisConf
  }

  lazy val session: SparkSession = SparkSession.builder.config(config).enableHiveSupport().getOrCreate()
}
