package ai.starlake.config

import com.typesafe.scalalogging.LazyLogging

object SparkSessionBuilder extends LazyLogging {
  val sparkConnectUrl: Option[String] =
    sys.env.get("SL_SPARK_CONNECT_URI").orElse(sys.env.get("SPARK_REMOTE"))
  val isSparkConnectActive: Boolean = sparkConnectUrl.isDefined

  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession
  def build(config: SparkConf): SparkSession = {
    sparkConnectUrl match {
      case Some(uri) =>
        val builder = SparkSession.builder()
        // Forward config entries compatible with Spark Connect
        config.getAll.foreach { case (k, v) => builder.config(k, v) }
        val session =
          try {
            classOf[SparkSession.Builder]
              .getDeclaredMethod("remote", classOf[String])
              .invoke(builder, uri)
              .asInstanceOf[SparkSession.Builder]
              .getOrCreate()
          } catch {
            case _: NoSuchMethodException =>
              throw new UnsupportedOperationException(
                "Spark Connect requires Spark 3.4+. The 'remote' method was not found on SparkSession.Builder."
              )
          }
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
        val localCatalog = config.getOption("spark.localCatalog").getOrElse("none")
        // sys.env.getOrElse("SL_SPARK_LOCAL_CATALOG", "none")
        val session =
          if (localCatalog != "none") {
            SparkSession.builder().config(config).master(master).enableHiveSupport().getOrCreate()
          } else
            SparkSession.builder().config(config).master(master).getOrCreate()
        logger.info("Spark Version -> " + session.version)
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
        session
    }
  }
}
