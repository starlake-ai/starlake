package com.ebiznext.comet.job.metrics

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.job.metrics.Metrics.{ContinuousMetric, DiscreteMetric}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.{Domain, Schema}
import com.ebiznext.comet.utils.SparkJob
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.col

/** To record statistics with other information during ingestion.
  *
  */

class MetricsJob(
  datasetPath: Path,
  domain: Domain,
  schema: Schema,
  stage: String,
  storageHandler: StorageHandler
) extends SparkJob {

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
    * @param standarddev           : Standard deviation metric
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
    min: Option[Long],
    max: Option[Long],
    mean: Option[Long],
    count: Option[Long],
    missingValues: Option[Long],
    variance: Option[Double],
    standarddev: Option[Double],
    sum: Option[Double],
    skewness: Option[Double],
    kurtosis: Option[Double],
    percentile25: Option[Double],
    median: Option[Double],
    percentile75: Option[Double],
    category: Option[List[String]],
    countDistinct: Option[Int],
    countByCategory: Option[Map[String, Long]],
    frequencies: Option[Map[String, Double]],
    missingValuesDiscrete: Option[Long],
    timestamp: Long,
    stage: String
  )

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
    * @tparam T           :   Return type of the function
    * @return             :   T, Long for CountDiscrete and Double for Frequencies
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
    * Saves a dataset. if the path exists already, save to an intermediary directory because
    * spark cant read and write to the same directory
    *
    * @param dataToSave :   dataset to be saved
    * @param path       :   Path to save the file at
    */
  def saveDataset(dataToSave: DataFrame, path: Path): Unit = {
    if (storageHandler.exist(path)) {
      val pathIntermediate = new Path(path, ".stat")

      val dataByVariableStored: DataFrame = session.read.parquet(path.toString).union(dataToSave)
      dataByVariableStored.coalesce(1).write.mode("append").parquet(pathIntermediate.toString)
      storageHandler.delete(path)
      storageHandler.move(pathIntermediate, path)
      logger.whenDebugEnabled {
        session.read.parquet(path.toString).show(1000, truncate = false)
      }
    } else
      dataToSave.coalesce(1).write.mode("append").parquet(path.toString)
  }

  /**
    * Function to get metric
    *
    * @param metric   : metric we're looking for
    * @param colName  : Column name
    * @tparam T       : Type of the metric value
    * @return         : Option of T either Double or Long depending on the metric.
    */
  def getMetric[T: Manifest](metric: Dataset[Row], colName: String): Option[T] = {
    metric.count() match {
      case 1 => Some(metric.select(colName).first().getAs[T](0))
      case _ => None
    }
  }

  /** Function that get String type metric
    *
    * @param metric   : the dataset
    * @param colName  : the column name
    * @return         : the metric in Option type of List of String
    */
  def getMetricListStringType(metric: Dataset[Row], colName: String): Option[List[String]] = {
    metric.count() match {
      case 1 => None
      case _ => Some(metric.select("Category").collect.map(_.getString(0)).toList)
    }
  }

  /** Function that get countByCategory and frequencies metrics
    *
    * @param dfStatistics : the dataframe
    * @param colName      : the column name
    * @return             : the metric in Option type of Map[String,A]
    */
  def getMetricCount[A](
    dfStatistics: DataFrame,
    colName: String,
    threshold: Int,
    metric: String,
    f: (DataFrame, String, String, String) => A
  ): Option[Map[String, A]] = {
    dfStatistics.filter(col("Variables").isin(colName)).count() match {
      case _ if getListCategory(dfStatistics, colName).length - threshold < 0 =>
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
    dfStatistics.filter(col("Variables").isin(colName)).count() match {
      case _ if getListCategory(dfStatistics, colName).length - threshold < 0 =>
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
    * @param dfStatistics : the dataframe
    * @param colName      : the column name
    * @return             : the metric in Option type of Int
    */
  def getMetricCountDistinct(dfStatistics: DataFrame, colName: String): Option[Int] = {
    dfStatistics.filter(col("Variables").isin(colName)).count() match {
      case 1 => None
      case _ => Some(getListCategory(dfStatistics, colName).length)
    }
  }

  /** Function to save the dfStatistics to parquet format.
    *
    * @param dfStatistics      : Dataframe that contains all the computed metrics
    * @param domain            : name of the domain
    * @param schema            : schema of the initial data
    * @param ingestionTime     : time which correspond to the ingestion
    * @param stageState        : stage (unit / global)
    * @param path              : path where to save the file
    * @param threshold         : The limit value for the number of sub-class to consider
    * @return : the stored dataframe version of the parquet file
    */
  def saveDataStatToParquet(
    dfStatistics: DataFrame,
    domain: Domain,
    schema: Schema,
    ingestionTime: Timestamp,
    stageState: String,
    path: Path,
    threshold: Int
  ): Unit = {

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
        getMetric[Long](metric, "Min"),
        getMetric[Long](metric, "Max"),
        getMetric[Long](metric, "Mean"),
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
        getMetricCount[Long](dfStatistics, c, threshold, "CountDiscrete", getCategoryMetric),
        getMetricCount[Double](dfStatistics, c, threshold, "Frequencies", getCategoryMetric),
        getMetricCountMissValuesDiscrete(dfStatistics, c, threshold),
        ingestionTime,
        stageState
      )
    }

    import session.implicits._

    val dataByVariable = listRowByVariable.toDF

    saveDataset(dataByVariable, path)

  }

  override def name: String = "Json Stat Job"

  def metricsPath(path: String): Path = {
    new Path(
      path
        .replace("{domain}", domain.name)
        .replace("{schema}", schema.name)
        .replace("{dataset}", datasetPath.toString)
    )
  }

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  // TODO Implement stageState
  override def run(): SparkSession = {
    val dataUse: DataFrame = session.read.parquet(datasetPath.toString)
    val attributes: List[String] = schema.discreteAttrs().map(_.name)
    val discreteOps: List[DiscreteMetric] = Metrics.discreteMetrics
    val continuousOps: List[ContinuousMetric] = Metrics.continuousMetrics
    val stageState: String = ???
    val metricsPath: Path = new Path(Settings.comet.metrics.path)

    val discreteDataset =
      Metrics.computeDiscretMetric(dataUse, attributes, discreteOps)
    val continuousDataset =
      Metrics.computeContinuiousMetric(dataUse, schema.continuousAttrs().map(_.name), continuousOps)

    val savePath = new Path(metricsPath, new Path(domain.name, schema.name))
    storageHandler.mkdirs(savePath)
    this.saveDataStatToParquet(
      discreteDataset.union(continuousDataset),
      domain,
      schema,
      storageHandler.lastModified(datasetPath),
      stageState,
      savePath,
      Settings.comet.metrics.discreteMaxCardinality
    )

    session
  }
}
