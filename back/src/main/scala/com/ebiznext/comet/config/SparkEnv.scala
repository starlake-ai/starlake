package com.ebiznext.comet.config

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Any Spark Job will inherit from this class.
  * All properties defined in application conf file and prefixed by the "spark" key will be loaded into the Spark Job
  * @param name: Cusom spark application name prefix. The current datetime is appended this the Spark Job name
  */
class SparkEnv(name: String) extends StrictLogging {

  /**
    * Load spark.* properties rom the application conf file
    */
  val config = {
    val now = LocalDateTime
      .now()
      .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss.SSS"))
    val appName = s"$name-$now"
    val thisConf = new SparkConf().setAppName(appName)

    import scala.collection.JavaConversions._
    ConfigFactory
      .load()
      .getConfig("spark")
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

  /**
    * Creates a Spark Session with the spark.* keys defined the applciation conf file.
    */
  lazy val session: SparkSession =
    SparkSession.builder.config(config).getOrCreate()
}
