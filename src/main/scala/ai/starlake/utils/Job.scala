package ai.starlake.utils

import ai.starlake.config.{Settings, SparkEnv, UdfRegistration}
import ai.starlake.schema.model.Metadata
import better.files.File
import com.google.gson.Gson
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.jdk.CollectionConverters.mapAsJavaMapConverter
import scala.util.{Failure, Success, Try}

trait JobResult {
  def prettyPrint(
    format: String,
    headers: List[String],
    values: List[List[String]],
    output: Option[File]
  ): Unit = {
    format match {
      case "csv" =>
        (headers :: values).foreach { row =>
          output.foreach(_.appendLine(row.toString.mkString(",")))
          println(row.mkString(","))
        }

      case "table" =>
        headers :: values match {
          case Nil =>
            output.foreach(_.appendLine("Result is empty."))
            println("Result is empty.")
          case _ =>
            output.foreach(_.appendLine(TableFormatter.format(headers :: values)))
            println(TableFormatter.format(headers :: values))
        }

      case "json" =>
        values.foreach { value =>
          val map = headers.zip(value).toMap
          val json = new Gson().toJson(map.asJava)
          output.foreach(_.appendLine(json))
          println(json)
        }
    }
  }
}

case class SparkJobResult(dataframe: Option[DataFrame], rejectedCount: Long = 0L) extends JobResult
case class JdbcJobResult(headers: List[String], rows: List[List[String]] = Nil) extends JobResult {
  def show(format: String, rootServe: scala.Option[String]): Unit = {
    val output = rootServe.map(File(_, "run.log"))
    output.foreach(_.overwrite(s""))
    prettyPrint(format, headers, rows, output)
  }

}

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
    val udfs = settings.appConfig.getUdfs()
    udfs.foreach(registerUdf)
    sparkEnv.session
  }

  // TODO Should we issue a warning if used with Overwrite mode ????
  // TODO Check that the year / month / day / hour / minute do not already exist
  private def buildPartitionedDF(dataset: DataFrame, cols: List[String]): DataFrame = {
    var partitionedDF = dataset.withColumn("sl_date", current_timestamp())
    val dataSetsCols = dataset.columns.toList
    cols.foreach {
      case "sl_date" if !dataSetsCols.contains("date") =>
        partitionedDF = partitionedDF.withColumn(
          "date",
          date_format(col("sl_date"), "yyyyMMdd").cast(IntegerType)
        )
      case "sl_year" if !dataSetsCols.contains("year") =>
        partitionedDF = partitionedDF.withColumn("year", year(col("sl_date")))
      case "sl_month" if !dataSetsCols.contains("month") =>
        partitionedDF = partitionedDF.withColumn("month", month(col("sl_date")))
      case "sl_day" if !dataSetsCols.contains("day") =>
        partitionedDF = partitionedDF.withColumn("day", dayofmonth(col("sl_date")))
      case "sl_hour" if !dataSetsCols.contains("hour") =>
        partitionedDF = partitionedDF.withColumn("hour", hour(col("sl_date")))
      case "sl_minute" if !dataSetsCols.contains("minute") =>
        partitionedDF = partitionedDF.withColumn("minute", minute(col("sl_date")))
      case _ =>
        partitionedDF
    }
    partitionedDF.drop("sl_date")
  }

  /** Partition a dataset using dataset columns. To partition the dataset using the ingestion time,
    * use the reserved column names :
    *   - sl_date
    *   - sl_year
    *   - sl_month
    *   - sl_day
    *   - sl_hour
    *   - sl_minute These columns are renamed to "date", "year", "month", "day", "hour", "minute" in
    *     the dataset and their values is set to the current date/time.
    *
    * @param dataset
    *   : Input dataset
    * @param partition
    *   : list of columns to use for partitioning.
    * @return
    *   The Spark session used to run this job
    */
  protected def partitionedDatasetWriter(
    dataset: DataFrame,
    partition: List[String]
  ): DataFrameWriter[Row] = {
    partition match {
      case Nil => dataset.write
      case cols if cols.forall(Metadata.CometPartitionColumns.contains) =>
        val strippedCols = cols.map(_.substring("sl_".length))
        val partitionedDF = buildPartitionedDF(dataset, cols)
        // does not work on nested fields -> https://issues.apache.org/jira/browse/SPARK-18084
        partitionedDF.write.partitionBy(strippedCols: _*)
      case cols if !cols.exists(Metadata.CometPartitionColumns.contains) =>
        dataset.write.partitionBy(cols: _*)
      case _ =>
        // Should never happend
        // TODO Test this at load time
        throw new Exception("Cannot mix comet & non comet col names")

    }
  }

  protected def partitionDataset(dataset: DataFrame, partition: List[String]): DataFrame = {
    logger.info(s"""Partitioning on ${partition.mkString(",")}""")
    partition match {
      case Nil => dataset
      case cols if cols.forall(Metadata.CometPartitionColumns.contains) =>
        buildPartitionedDF(dataset, cols)
      case cols if !cols.exists(Metadata.CometPartitionColumns.contains) =>
        dataset
      case _ =>
        dataset

    }
  }

  protected def analyze(fullTableName: String): Any = {
    if (settings.appConfig.analyze) {
      logger.info(s"computing statistics on table $fullTableName")
      val allCols = session.table(fullTableName).columns.mkString(",")
      session.table(fullTableName)
      val partitionedCols =
        Try {
          val partitionedColsDF = session.sql(s"show partitions $fullTableName")
          import session.implicits._
          val partitionedCols = partitionedColsDF
            .map(_.getAs[String](0))
            .first
            .split('/')
            .map(_.split("=")(0))
            .toList
            .mkString(",")
          Some(s"ANALYZE TABLE $fullTableName PARTITION ($partitionedCols) COMPUTE STATISTICS")
        } match {
          case Success(value) =>
            value
          case Failure(e) =>
            // Ignore errors when trying to compute statistics non partitioned table
            logger.info(Utils.exceptionAsString(e))
            None
        }

      if (session.version.substring(0, 3).toDouble >= 2.4) {
        val analyzeCommands =
          List(
            Some(s"ANALYZE TABLE $fullTableName COMPUTE STATISTICS NOSCAN"),
            partitionedCols,
            Some(s"ANALYZE TABLE $fullTableName COMPUTE STATISTICS FOR COLUMNS $allCols")
          ).flatten
        analyzeCommands.foreach { command =>
          Try {
            session.sql(command)
          } match {
            case Success(df) => df
            case Failure(e) =>
              logger.warn(
                s"Failed to compute statistics for table $fullTableName on columns $allCols"
              )
              e.printStackTrace()
          }
        }
      }
    }
  }
}
