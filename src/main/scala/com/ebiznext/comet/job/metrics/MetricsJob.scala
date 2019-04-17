package com.ebiznext.comet.job.metrics

import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.job.metrics.Metrics.{ContinuousMetric, DiscreteMetric}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.{Domain, Schema, Stage}
import com.ebiznext.comet.utils.SparkJob
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

/**
  *
  * @param domain                : Domain name
  * @param schema                : Schema
  * @param variableName          : Variable name from the attributes
  * @param min                   : Min metric
  * @param max                   : Max metric
  * @param mean                  : Mean metric
  * @param count                 : Count metric
  * @param missingValues         : Missing Values metric
  * @param variance              : Variance metric
  * @param standardDev           : Standard deviation metric
  * @param sum                   : Sum metric
  * @param skewness              : Skewness metric
  * @param kurtosis              : Kurtosis metric
  * @param percentile25          : Percentile25 metric
  * @param median                : Median metric
  * @param percentile75          : Percentile75 metric
  * @param category              : Category metric
  * @param countDistinct         : Count Distinct metric
  * @param countByCategory       : Count By Category metric
  * @param frequencies           : Frequency metric
  * @param missingValuesDiscrete : Missing Values Discrete metric
  */
case class MetricRow(
  domain: String,
  schema: String,
  variableName: String,
  min: Option[Double],
  max: Option[Double],
  mean: Option[Double],
  count: Option[Long],
  missingValues: Option[Long],
  variance: Option[Double],
  standardDev: Option[Double],
  sum: Option[Double],
  skewness: Option[Double],
  kurtosis: Option[Double],
  percentile25: Option[Double],
  median: Option[Double],
  percentile75: Option[Double],
  category: Option[List[String]],
  countDistinct: Option[Long],
  countByCategory: Option[Map[String, Long]],
  frequencies: Option[Map[String, Double]],
  missingValuesDiscrete: Option[Long],
  timestamp: Long,
  stage: Stage
)

/** To record statistics with other information during ingestion.
  *
  */

/**
  *
  * @param domain         : Domain name
  * @param schema         : Schema
  * @param stage          : stage
  * @param storageHandler : Storage Handler
  */
class MetricsJob(
  domain: Domain,
  schema: Schema,
  stage: Stage,
  storageHandler: StorageHandler
) extends SparkJob {

  /** Function that retrieves class names for each variable (case discrete variable)
    *
    * @param dataInit     : Dataframe that contains all the computed metrics
    * @param nameVariable : name of the variable
    * @return : list of all class associates to the variable
    */

  def getListCategory(dataInit: DataFrame, nameVariable: String): List[String] = {
    val dataReduceCategory: DataFrame = dataInit.filter(col("Variables").isin(nameVariable))
    dataReduceCategory.select("Category").collect.map(_.getString(0)).toList

  }

  /**
    * Gets the categorical metric as type T
    *
    * @param dataInit     :   Initial dataset
    * @param nameVariable :   Name of the variable
    * @param nameCategory :   Name of the category
    * @param metric       :   Name of the metric to compute
    * @tparam T :   Return type of the function
    * @return :   T, Long for CountDiscrete and Double for Frequencies
    */
  def getCategoryMetric[T: Manifest](
    dataInit: DataFrame,
    nameVariable: String,
    nameCategory: String,
    metric: String
  ): T = {
    val dataReduceCategory: DataFrame = dataInit.filter(col("Variables").isin(nameVariable))
    val dataCategory: DataFrame = dataReduceCategory.filter(col("Category").isin(nameCategory))
    dataCategory.select(col(metric)).first().getAs[T](0)
  }

  /**
    * Saves a dataset. if the path is empty (the first time we call metrics on the schema) then we can write
    * if there's already parquet files stored in it, then create a temporary directory to compute on, and flush
    * the path to move updated metrics in it
    *
    * @param dataToSave :   dataset to be saved
    * @param path       :   Path to save the file at
    */
  def save(dataToSave: DataFrame, path: Path): Unit = {
    if (storageHandler.exist(path)) {
      val pathIntermediate = new Path(path.getParent, ".metrics")
      val dataByVariableStored: DataFrame = session.read
        .parquet(path.toString)
        .union(dataToSave)
      dataByVariableStored
        .coalesce(1)
        .write
        .mode("append")
        .parquet(pathIntermediate.toString)
      storageHandler.delete(path)
      storageHandler.move(pathIntermediate, path)
      logger.whenDebugEnabled {
        session.read.parquet(path.toString).show(1000, truncate = false)
      }
    } else {
      storageHandler.mkdirs(path)
      dataToSave
        .coalesce(1)
        .write
        .mode("append")
        .parquet(path.toString)

    }
  }

  /**
    * Function to get metric
    *
    * @param metric  : metric we're looking for
    * @param colName : Column name
    * @tparam T : Type of the metric value
    * @return : Option of T either Double or Long depending on the metric.
    */
  def getMetric[T: Manifest](metric: DataFrame, colName: String): Option[T] = {
    metric.select("_metricType_").first().getString(0) match {
      case "Continuous" => Some(metric.select(colName).first().getAs[T](0))
      case _            => None
    }
  }

  /** Function that get String type metric
    *
    * @param metric  : the dataset
    * @param colName : the column name
    * @return : the metric in Option type of List of String
    */
  def getMetricListStringType(metric: DataFrame, colName: String): Option[List[String]] = {
    metric.select("_metricType_").first().getString(0) match {
      case "Discrete" => Some(metric.select("Category").collect.map(_.getString(0)).toList)
      case _          => None
    }
  }

  /** Function that get countByCategory and frequencies metrics
    *
    * @param dfStatistics : the dataframe
    * @param colName      : the column name
    * @return : the metric in Option type of Map[String,A]
    */
  def getMetricCount[A](
    dfStatistics: DataFrame,
    colName: String,
    threshold: Int,
    metric: String,
    f: (DataFrame, String, String, String) => A
  ): Option[Map[String, A]] = {
    dfStatistics.select("_metricType_").first().getString(0) match {
      case "Discrete" if getListCategory(dfStatistics, colName).length - threshold < 0 =>
        Some(
          Map(
            getListCategory(dfStatistics, colName)
              .map(x => x -> f(dfStatistics, colName, x, metric)): _*
          )
        )
      case _ => None
    }
  }

  /** Function that get missingValuesDiscrete variable metric
    *
    * @param dfStatistics : the dataframe
    * @param colName      : the column name
    * @param threshold    : the threshold
    * @return : the metric in Option type of Long
    */
  def getMetricCountMissValuesDiscrete(
    dfStatistics: DataFrame,
    colName: String,
    threshold: Int
  ): Option[Long] = {
    dfStatistics.select("_metricType_").first().getString(0) match {
      case "Discrete" if getListCategory(dfStatistics, colName).length - threshold < 0 =>
        Some(
          dfStatistics
            .filter(col("Variables").isin(colName))
            .select("CountMissValuesDiscrete")
            .first()
            .getLong(0)
        )
      case _ => None
    }
  }

  /** Function that get countDistinct variable metric
    *
    * @param metric  : the dataframe
    * @param colName : the column name
    * @return : the metric in Option type of Int
    */
  def getMetricCountDistinct(metric: DataFrame, colName: String): Option[Long] = {
    metric.select("_metricType_").first().getString(0) match {
      case "Discrete" => Some(getListCategory(metric, colName).length)
      case _          => None
    }
  }

  /** Function that unify metric dataframes schemas on MetricRow case class, then write save the result to parquet
    *
    * @param listDfStats   : List of dataframes that contains metrics of each type (continuous and discrete)
    * @param domain        : name of the domain
    * @param schema        : schema of the initial data
    * @param ingestionTime : time which correspond to the ingestion
    * @param stageState    : stage (unit / global)
    * @param threshold     : The limit value for the number of sub-class to consider
    */
  def extractMetrics(
    listDfStats: List[DataFrame],
    domain: Domain,
    schema: Schema,
    ingestionTime: Timestamp,
    stageState: Stage,
    threshold: Int
  ): DataFrame = {

    listDfStats
      .map { dfStatistics =>
        val listVariable: List[String] = dfStatistics
          .select("Variables")
          .collect
          .map(_.getString(0))
          .toList
          .distinct
        val listRowByVariable = listVariable.map { c =>
          val metric = dfStatistics.filter(col("Variables").isin(c))
          MetricRow(
            domain.name,
            schema.name,
            c,
            getMetric[Double](metric, "Min"),
            getMetric[Double](metric, "Max"),
            getMetric[Double](metric, "Mean"),
            getMetric[Long](metric, "Count"),
            getMetric[Long](metric, "CountMissValues"),
            getMetric[Double](metric, "Var"),
            getMetric[Double](metric, "Stddev"),
            getMetric[Double](metric, "Sum"),
            getMetric[Double](metric, "Skewness"),
            getMetric[Double](metric, "Kurtosis"),
            getMetric[Double](metric, "Percentile25"),
            getMetric[Double](metric, "Median"),
            getMetric[Double](metric, "Percentile75"),
            getMetricListStringType(metric, "Category"),
            getMetricCountDistinct(dfStatistics, c),
            getMetricCount[Long](
              dfStatistics,
              c,
              threshold,
              "CountDiscrete",
              getCategoryMetric[Long]
            ),
            getMetricCount[Double](
              dfStatistics,
              c,
              threshold,
              "Frequencies",
              getCategoryMetric[Double]
            ),
            getMetricCountMissValuesDiscrete(dfStatistics, c, threshold),
            ingestionTime,
            stageState
          )
        }

        session.createDataFrame(listRowByVariable)

      }
      .reduce(_.union(_))
  }

  override def name: String = "Compute metrics job"

  /** Function to build the metrics save path
    *
    * @param path : path where metrics are stored
    * @return : path where the metrics for the specified schema are stored
    */
  def getMetricsPath(path: String): Path = {
    new Path(
      path
        .replace("{domain}", domain.name)
        .replace("{schema}", schema.name)
    )
  }

  /** Function that enforce type on certain discrete metrics column to avoid types casts problems
    * The types we enforce are the same as those in the case class MetricRow attributes.
    *
    * @param statsDf the dataframe we want to type
    * @return typed dataframe
    */
  def discreteMetricTyping(statsDf: DataFrame): DataFrame = {
    statsDf
      .withColumn("Min", statsDf.col("Min").cast(DoubleType))
      .withColumn("Max", statsDf.col("Max").cast(DoubleType))
      .withColumn("Mean", statsDf.col("Mean").cast(DoubleType))
      .withColumn("Var", statsDf.col("Var").cast(DoubleType))
      .withColumn("Stddev", statsDf.col("Stddev").cast(DoubleType))
      .withColumn("Sum", statsDf.col("Sum").cast(DoubleType))
      .withColumn("Skewness", statsDf.col("Skewness").cast(DoubleType))
      .withColumn("Kurtosis", statsDf.col("Kurtosis").cast(DoubleType))
      .withColumn("Percentile25", statsDf.col("Percentile25").cast(DoubleType))
      .withColumn("Median", statsDf.col("Median").cast(DoubleType))
      .withColumn("Percentile75", statsDf.col("Percentile75").cast(DoubleType))
  }

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): SparkSession = {
    val datasetPath = new Path(DatasetArea.accepted(domain.name), schema.name)
    val dataUse: DataFrame = session.read.parquet(datasetPath.toString)
    run(dataUse, storageHandler.lastModified(datasetPath))
  }

  def run(dataUse: DataFrame, timestamp: Timestamp): SparkSession = {
    val discAttrs: List[String] = schema.discreteAttrs().map(_.getFinalName())
    val continAttrs: List[String] = schema.continuousAttrs().map(_.getFinalName())

    val discreteOps: List[DiscreteMetric] = Metrics.discreteMetrics
    val continuousOps: List[ContinuousMetric] = Metrics.continuousMetrics
    val savePath: Path = getMetricsPath(Settings.comet.metrics.path)

    val discreteDataset =
      Metrics.computeDiscretMetric(dataUse, discAttrs, discreteOps)
    val continuousDataset =
      discreteMetricTyping(Metrics.computeContinuousMetric(dataUse, continAttrs, continuousOps))

    val allMetricsDf = extractMetrics(
      List(discreteDataset, continuousDataset),
      domain,
      schema,
      timestamp,
      stage,
      Settings.comet.metrics.discreteMaxCardinality
    )

    save(allMetricsDf, savePath)
    session
  }
}
