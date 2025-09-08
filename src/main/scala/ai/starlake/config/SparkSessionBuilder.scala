package ai.starlake.config

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionBuilder extends LazyLogging {
  val sparkConnectUrl: Option[String] = sys.env.get("SL_SPARK_CONNECT_URI")
  val isSparkConnectActive: Boolean = sparkConnectUrl.isDefined

  def build(config: SparkConf): SparkSession = {
    sparkConnectUrl match {
      case Some(uri) =>
        val builder = SparkSession.builder()
        val session =
          classOf[SparkSession.Builder]
            .getDeclaredMethod("remote", classOf[String])
            .invoke(builder, uri)
            .asInstanceOf[SparkSession.Builder]
            .getOrCreate()
        Runtime.getRuntime.addShutdownHook(new Thread() {
          override def run(): Unit = {
            session.close()
          }
        })
        logger.info("Spark Version -> " + session.version)
        session
      case None =>
        val master =
          config.get("spark.master", sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
        val session =
          if (
            sys.env.getOrElse("SL_SPARK_NO_CATALOG", "false").toBoolean &&
            config.getOption("spark.sql.catalogImplementation").isEmpty
          ) {
            SparkSession.builder().config(config).master(master).enableHiveSupport().getOrCreate()
          } else
            SparkSession.builder().config(config).master(master).getOrCreate()
        logger.info("Spark Version -> " + session.version)
        if (!isSparkConnectActive) {
          val hadoopConf = session.sparkContext.hadoopConfiguration
          sys.env.get("SL_STORAGE_CONF").foreach { value =>
            value
              .split(',')
              .map { x =>
                val t = x.split('=')
                t(0).trim -> t(1).trim
              }
              .foreach { case (k, v) => hadoopConf.set(k, v) }
          }
        }
        session
    }
  }
}
