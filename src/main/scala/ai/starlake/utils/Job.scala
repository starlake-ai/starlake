package ai.starlake.utils

import ai.starlake.config.{Settings, SparkEnv, UdfRegistration}
import ai.starlake.schema.model.SinkType.{BQ, FS, JDBC, KAFKA}
import ai.starlake.schema.model.{Metadata, SinkType, Views}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Formatter._
import ai.starlake.utils.kafka.KafkaClient
import com.hubspot.jinjava.Jinjava
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait JobResult

case class AirflowJobResult(response: String) extends JobResult

case class SparkJobResult(dataframe: Option[DataFrame]) extends JobResult

/** All Spark Job extend this trait. Build Spark session using spark variables from
  * application.conf.
  */

trait JobBase extends StrictLogging with DatasetLogging {
  def name: String
  implicit def settings: Settings

  /** Just to force any job to implement its entry point using within the "run" method
    *
    * @return
    *   : Spark Dataframe for Spark Jobs None otherwise
    */
  def run(): Try[JobResult]

  type JdbcConfigName = String

  /** @param valueWithEnv
    *   in the form [SinkType:[configName:]]viewName
    * @return
    *   (SinkType, configName, viewName)
    */
  protected def parseViewDefinition(
    valueWithEnv: String
  ): (SinkType, Option[JdbcConfigName], String) = {
    val sepIndex = valueWithEnv.indexOf(":")
    if (sepIndex > 0) {
      val key = valueWithEnv.substring(0, sepIndex)
      val sepConfigIndex = valueWithEnv.indexOf(':', sepIndex + 1)
      if (sepConfigIndex > 0) {
        (
          SinkType.fromString(key),
          Some(valueWithEnv.substring(sepIndex + 1, sepConfigIndex)),
          valueWithEnv.substring(sepConfigIndex + 1)
        )
      } else
        (SinkType.fromString(key), None, valueWithEnv.substring(sepIndex + 1))
    } else // parquet is the default
      (SinkType.FS, None, valueWithEnv)
  }
  protected val jinjava = {
    val res = new Jinjava()
    res.setResourceLocator(new JinjaResourceHandler())
    res
  }

  protected def parseJinja(str: String, params: Map[String, String]): String = parseJinja(
    List(str),
    params
  ).head

  protected def parseJinja(str: List[String], params: Map[String, String]): List[String] = {
    val result = str.map { sql =>
      CommentParser.stripComments(
        jinjava
          .render(sql, params.asJava)
          .richFormat(params, Map.empty)
          .trim
      )
    }
    result
  }

}

trait SparkJob extends JobBase {

  protected def withExtraSparkConf(sourceConfig: SparkConf): SparkConf = {
    // During Job execution, schema update are done on the table before data is written
    // These two options below are thus disabled.
    // We disable them because even though the user asked for WRITE_APPEND
    // On merge, we write in WRITE_TRUNCATE mode.
    // Moreover, since we handle schema validaty through the YAML file, we manage these settings automatically
    sourceConfig.remove("spark.datasource.bigquery.allowFieldAddition")
    sourceConfig.remove("spark.datasource.bigquery.allowFieldRelaxation")

    val now = LocalDateTime
      .now()
      .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss.SSS"))

    val appName = s"$name-$now"
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
    val udfs = settings.comet.udfs.map { udfs =>
      udfs.split(',').toList
    } getOrElse Nil
    udfs.foreach(registerUdf)
    sparkEnv.session
  }

  // TODO Should we issue a warning if used with Overwrite mode ????
  // TODO Check that the year / month / day / hour / minute do not already exist
  private def buildPartitionedDF(dataset: DataFrame, cols: List[String]): DataFrame = {
    var partitionedDF = dataset.withColumn("comet_date", current_timestamp())
    val dataSetsCols = dataset.columns.toList
    cols.foreach {
      case "comet_date" if !dataSetsCols.contains("date") =>
        partitionedDF = partitionedDF.withColumn(
          "date",
          date_format(col("comet_date"), "yyyyMMdd").cast(IntegerType)
        )
      case "comet_year" if !dataSetsCols.contains("year") =>
        partitionedDF = partitionedDF.withColumn("year", year(col("comet_date")))
      case "comet_month" if !dataSetsCols.contains("month") =>
        partitionedDF = partitionedDF.withColumn("month", month(col("comet_date")))
      case "comet_day" if !dataSetsCols.contains("day") =>
        partitionedDF = partitionedDF.withColumn("day", dayofmonth(col("comet_date")))
      case "comet_hour" if !dataSetsCols.contains("hour") =>
        partitionedDF = partitionedDF.withColumn("hour", hour(col("comet_date")))
      case "comet_minute" if !dataSetsCols.contains("minute") =>
        partitionedDF = partitionedDF.withColumn("minute", minute(col("comet_date")))
      case _ =>
        partitionedDF
    }
    partitionedDF.drop("comet_date")
  }

  /** Partition a dataset using dataset columns. To partition the dataset using the ingestion time,
    * use the reserved column names :
    *   - comet_date
    *   - comet_year
    *   - comet_month
    *   - comet_day
    *   - comet_hour
    *   - comet_minute These columns are renamed to "date", "year", "month", "day", "hour", "minute"
    *     in the dataset and their values is set to the current date/time.
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
        val strippedCols = cols.map(_.substring("comet_".length))
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
    if (settings.comet.analyze) {
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

  /** @param views
    * @param schemaHandler
    * @param sqlParameters
    * @return
    *   CTEs expressions that need to be injected in requests
    */
  protected def createSparkViews(
    views: Views,
    schemaHandler: SchemaHandler,
    sqlParameters: Map[String, String]
  ): List[String] = {
    // We parse the following strings
    // ex  BQ:[[ProjectID.]DATASET_ID.]TABLE_NAME"
    // or  BQ:[[ProjectID.]DATASET_ID.]TABLE_NAME.[comet_filter(col1 > 10 and col2 < 20)].[comet_select(col1, col2)]"
    // or  FS:/bucket/parquetfolder
    // or  JDBC:postgres:select *
    // or  KAFKA:topicConfigName
    // or  KAFKA:stream:topicConfigName
    views.views.flatMap { case (key, value) =>
      // Apply substitution defined with {{ }} and overload options in env by option in command line
      Option(value) match {
        case None =>
          val viewContent = schemaHandler
            .views(key)
            .getOrElse(throw new Exception(s"Unknown view $key"))

          val parsedContent = parseJinja(viewContent, schemaHandler.activeEnv() ++ sqlParameters)
          Some(s"$key AS ($parsedContent)")

        case Some(value) =>
          val valueWithEnv =
            parseJinja(value, schemaHandler.activeEnv() ++ sqlParameters)
              .richFormat(schemaHandler.activeEnv(), sqlParameters)
          val (sinkType, sinkConfig, path) = parseViewDefinition(valueWithEnv)
          logger.info(s"Loading view $path from $sinkType")
          val df: DataFrame = createSparkView(sinkType, sinkConfig, path)
          df.createOrReplaceTempView(key)
          logger.info(s"Created view $key")
          None
      }
    }.toList
  }

  protected def createSparkView(
    sinkType: SinkType,
    sinkConfig: Option[String],
    path: String
  ): DataFrame = {
    val df = sinkType match {
      case FS => // (FS, _, absolute_path|relative_path|sql)
        createFSView(path)
      case JDBC => // (JDBC, connectionName, queryString)
        createJDBCView(sinkConfig, path)
      case KAFKA => // (KAFKA, STREAM|FILE, topic)
        createKafkaView(sinkConfig, path)
      case BQ =>
        createBQView(path)
      case _ =>
        throw new Exception("Should never happen")
    }
    df
  }

  private def createJDBCView(sinkConfig: Option[String], path: String) = {
    val jdbcConfig =
      settings.comet.connections(sinkConfig.getOrElse(throw new Exception("")))
    session.read
      .options(jdbcConfig.options)
      .format(jdbcConfig.format)
      .option(JDBCOptions.JDBC_QUERY_STRING, path)
      .load()
      .cache()
  }

  private def createFSView(path: String) = {
    if (path.startsWith("/"))
      session.read.format(settings.comet.defaultFormat).load(path)
    else if (path.trim.toLowerCase.startsWith("select "))
      session.sql(path)
    else
      session.read
        .format(settings.comet.defaultFormat)
        .load(s"${settings.comet.datasets}/$path")
  }

  private def createBQView(path: String) = {
    val TablePathWithFilter = "(.*)\\.comet_filter\\((.*)\\)".r
    val TablePathWithSelect = "(.*)\\.comet_select\\((.*)\\)".r
    val TablePathWithFilterAndSelect =
      "(.*)\\.comet_select\\((.*)\\)\\.comet_filter\\((.*)\\)".r
    path match {
      case TablePathWithFilterAndSelect(tablePath, select, filter) =>
        logger
          .info(
            s"We are loading the Table with columns: $select and filters: $filter"
          )
        session.read
          .option("readDataFormat", "AVRO")
          .format("com.google.cloud.spark.bigquery")
          .option("table", tablePath)
          .option("filter", filter)
          .load()
          .selectExpr(select.replaceAll("\\s", "").split(","): _*)
          .cache()
      case TablePathWithFilter(tablePath, filter) =>
        logger.info(s"We are loading the Table with filters: $filter")
        session.read
          .option("readDataFormat", "AVRO")
          .format("com.google.cloud.spark.bigquery")
          .option("table", tablePath)
          .option("filter", filter)
          .load()
          .cache()
      case TablePathWithSelect(tablePath, select) =>
        logger.info(s"We are loading the Table with columns: $select")
        session.read
          .option("readDataFormat", "AVRO")
          .format("com.google.cloud.spark.bigquery")
          .option("table", tablePath)
          .load()
          .selectExpr(select.replaceAll("\\s", "").split(","): _*)
          .cache()
      case _ =>
        session.read
          .option("readDataFormat", "AVRO")
          .format("com.google.cloud.spark.bigquery")
          .option("table", path)
          .load()
          .cache()
    }
  }

  private def createKafkaView(sinkConfig: Option[String], path: String) = {
    sinkConfig match {
      case Some(x) if x.toLowerCase() == "stream" =>
        Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
          KafkaClient.consumeTopicStreaming(session, settings.comet.kafka.topics(path))
        }
      case _ =>
        Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
          val (dataframe, _) =
            kafkaClient.consumeTopicBatch(path, session, settings.comet.kafka.topics(path))
          dataframe
        }

    }
  }
}
