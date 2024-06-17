package ai.starlake.utils

import ai.starlake.config.{Settings, SparkEnv, UdfRegistration}
import better.files.File
import com.google.gson.Gson
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import java.io.{ByteArrayOutputStream, PrintStream}
import scala.jdk.CollectionConverters._
import scala.util.Try

trait JobResult {
  def prettyPrint(
    format: String,
    headers: List[String],
    values: List[List[String]]
  ): String = {
    val baos = new ByteArrayOutputStream()
    val printStream = new PrintStream(baos)

    format match {
      case "csv" =>
        (headers :: values).foreach { row =>
          printStream.println(row.mkString(","))
        }

      case "table" =>
        headers :: values match {
          case Nil =>
            printStream.println("Result is empty.")
          case _ =>
            printStream.println(TableFormatter.format(headers :: values))
        }

      case "json" =>
        val res = values.foreach { value =>
          val map = headers.zip(value).toMap.asJava
          val json = new Gson().toJson(map)
          printStream.println(json)
        }

      case "json-array" =>
        val res = values.map { value =>
          val map = headers.zip(value).toMap.asJava
          map
        }
        val json = new Gson().toJson(res.asJava)
        printStream.println(json)
    }
    baos.toString
  }
}

case class SparkJobResult(dataframe: Option[DataFrame], rejectedCount: Long = 0L) extends JobResult
case class JdbcJobResult(headers: List[String], rows: List[List[String]] = Nil) extends JobResult {
  def prettyPrint(format: String): String = prettyPrint(format, headers, rows)

  def show(format: String, rootServe: scala.Option[String]): Unit = {
    val output = rootServe.map(File(_, "extension.log"))
    output.foreach(_.append(s""))
    val result = prettyPrint(format, headers, rows)
    println(result)
  }

}
object JobResult {
  def empty: JobResult = EmptyJobResult
}
case object EmptyJobResult extends JobResult
case object FailedJobResult extends JobResult

/** All Spark Job extend this trait. Build Spark session using spark variables from
  * application.conf.
  */

trait JobBase extends StrictLogging with DatasetLogging {
  def name: String
  implicit def settings: Settings

  val appName =
    Option(System.getenv("SL_JOB_ID"))
      .orElse(settings.appConfig.jobIdEnvName.flatMap(e => Option(System.getenv(e))))
      .getOrElse(s"$name-${System.currentTimeMillis()}")

  def applicationId(): String = appName

  /** Just to force any job to implement its entry point using within the "run" method
    *
    * @return
    *   : Spark Dataframe for Spark Jobs None otherwise
    */
  def run(): Try[JobResult]

}

/** All Spark Job extend this trait. Build Spark session using spark variables from
  * application.conf. Make sure all variables are lazy since we do not want to build a spark session
  * for any of the other services
  */
trait SparkJob extends JobBase {

  protected def withExtraSparkConf(sourceConfig: SparkConf): SparkConf = {
    // During Job execution, schema update are done on the table before data is written
    // These two options below are thus disabled.
    // We disable them because even though the user asked for WRITE_APPEND
    // On merge, we write in WRITE_TRUNCATE mode.
    // Moreover, since we handle schema validaty through the YAML file, we manage these settings automatically
    sourceConfig.remove("spark.datasource.bigquery.allowFieldAddition")
    sourceConfig.remove("spark.datasource.bigquery.allowFieldRelaxation")
    settings.storageHandler().extraConf.foreach { case (k, v) =>
      sourceConfig.set("spark.hadoop." + k, v)
    }

    val thisConf = sourceConfig.setAppName(appName).set("spark.app.id", appName)
    logger.whenDebugEnabled {
      logger.debug(thisConf.toDebugString)
    }
    thisConf
  }

  private lazy val sparkEnv: SparkEnv = new SparkEnv(name, withExtraSparkConf)

  def getTableLocation(domain: String, schema: String): String = {
    getTableLocation(s"$domain.$schema")
  }

  def getTableLocation(fullTableName: String): String = {
    import session.implicits._
    session
      .sql(s"desc formatted $fullTableName")
      .toDF()
      .filter(Symbol("col_name") === "Location")
      .collect()(0)(1)
      .toString
  }

  protected def registerUdf(udf: String): Unit = {
    val udfInstance: UdfRegistration =
      Class
        .forName(udf)
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[UdfRegistration]
    udfInstance.register(sparkEnv.session)
  }

  lazy val session: SparkSession = {
    val udfs = settings.appConfig.getEffectiveUdfs()
    udfs.foreach(registerUdf)
    sparkEnv.session
  }

}
