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

  /**
    * Spark configuration
    */
  /** Function to get values of option
    *
    * @param x
    * @return
    */

  def getShow(x: Option[Any]) = x match {
    case Some(s) => s
    case None    => "?"
  }

  /** Function that retrieves class names for each variable (case discrete variable)
    *
    * @param dataInit     : Dataframe that contains all the computed metrics
    * @param nameVariable : name of the variable
    * @return : list of all class associates to the variable
    */

  def getListCategory(dataInit: DataFrame, nameVariable: String): (List[String], Int) = {
    val dataReduiceCategory: DataFrame = dataInit.filter(col("Variables").isin(nameVariable))
    val listCategory: List[String] =
      dataReduiceCategory.select("Category").collect.map(_.getString(0)).toList
    val lengthListCategory: Int = listCategory.length
    (listCategory, lengthListCategory)
  }

  /** Function that retrieves the values of the CountDiscrete metric (the distinct count) for one category  associate to one variable (case discrete variable)
    *
    * @param dataInit     : Dataframe that contains all the computed metrics
    * @param nameVariable : name of the variable
    * @param nameCategory : name of the category
    * @return : the value of the distinct count
    */

  def getMapCategoryCountDiscrete(
    dataInit: DataFrame,
    nameVariable: String,
    nameCategory: String
  ): Long = {
    val metricName: String = "CountDiscrete"
    val dataReduiceCategory: DataFrame = dataInit.filter(col("Variables").isin(nameVariable))
    val dataCategory: DataFrame = dataReduiceCategory.filter(col("Category").isin(nameCategory))
    dataCategory.select(col(metricName)).first().getLong(0)
  }

  /** Function that retrieves the values of the Frequencies metric (frequency) for one category  associate to one variable (case discrete variable)
    *
    * @param dataInit     : Dataframe that contains all the computed metrics
    * @param nameVariable : name of the variable
    * @param nameCategory : name of the category
    * @return : the value of the frequency
    */

  def getMapCategoryFrequencies(
    dataInit: DataFrame,
    nameVariable: String,
    nameCategory: String
  ): Double = {
    val metricName: String = "Frequencies"
    val dataReduiceCategory: DataFrame = dataInit.filter(col("Variables").isin(nameVariable))
    val dataCategory: DataFrame = dataReduiceCategory.filter(col("Category").isin(nameCategory))
    dataCategory.select(col(metricName)).first().getDouble(0)
  }

  /** Function to save the statDataFrame to parque the first time
    *
    * @param dataToSave : statDataFrame
    * @param path       : the path directory
    * @return : the resulting dataframe
    */

  def saveParquet(dataToSave: DataFrame, path: Path): DataFrame = {
    dataToSave.coalesce(1).write.mode("append").parquet(path.toString)
    dataToSave
  }

  /** Function to save the statDataFrame in the case it is not the first time
    *
    * @param dataToSave : statDataFrame
    * @param path       : the path directory
    * @return : the resulting dataframe
    */

  def saveDeleteParquet(dataToSave: DataFrame, path: Path) = {

    val pathIntermediate = new Path(path, "intermediateDirectory")

    val dataByVariableStored: DataFrame = session.read.parquet(path.toString).union(dataToSave)
    dataByVariableStored.coalesce(1).write.mode("append").parquet(pathIntermediate.toString)
    storageHandler.delete(path)
    storageHandler.move(pathIntermediate, path)
    logger.whenDebugEnabled {
      session.read.parquet(path.toString).show(1000, false)
    }
    dataByVariableStored
  }

  /** Function that get Long type metric
    *
    * @param metric  : the dataset
    * @param colName : the column name
    * @return : the metric in Option type of Long
    */
  def getMetricLongType(metric: Dataset[Row], colName: String): Option[Long] = {
    metric.count() match {
      case 1 => Some(metric.select(colName).first().getLong(0))
      case _ => None
    }
  }

  /** Function that get Double type metric
    *
    * @param metric  : the dataset
    * @param colName : the column name
    * @return : the metric in Option type of Double
    */
  def getMetricDoubleType(metric: Dataset[Row], colName: String): Option[Double] = {
    metric.count() match {
      case 1 => Some(metric.select(colName).first().getDouble(0))
      case _ => None
    }
  }

  /** Function that get String type metric
    *
    * @param metric  : the dataset
    * @param colName : the column name
    * @return : the metric in Option type of List of String
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
    * @return : the metric in Option type of Map[String,A]
    */
  def getMetricCount[A](
    dfStatistics: DataFrame,
    colName: String,
    threshold: Int,
    f: (DataFrame, String, String) => A
  ): Option[Map[String, A]] = {
    dfStatistics.filter(col("Variables").isin(colName)).count() match {
      case _ if getListCategory(dfStatistics, colName)._2 - threshold < 0 =>
        Some(
          Map(
            (getListCategory(dfStatistics, colName)._1)
              .map(x => (x -> f(dfStatistics, colName, x))): _*
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
  def getMetricCountMissValuesDiscrete(dfStatistics: DataFrame, colName: String, threshold: Int) = {
    dfStatistics.filter(col("Variables").isin(colName)).count() match {
      case _ if getListCategory(dfStatistics, colName)._2 - threshold < 0 =>
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
    * @return : the metric in Option type of Int
    */
  def getMetricCountDistinct(dfStatistics: DataFrame, colName: String) = {
    dfStatistics.filter(col("Variables").isin(colName)).count() match {
      case 1 => None
      case _ => Some(getListCategory(dfStatistics, colName) _2)
    }
  }

  /** Function to save the dfStatistics to parquet format.
    *
    * @param dfStatistics      : Dataframe that contains all the computed metrics
    * @param domain            : name of the domain
    * @param schema            : schema of the initial data
    * @param timeIngestion     : time which correspond to the ingestion
    * @param stageState        : stage (unit / global)
    * @param path              : path where to save the file
    * @param saveDirectoryName : The name of the saving directory
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
  ): DataFrame = {

    val listVariable: List[String] = dfStatistics
      .select("Variables")
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
        getMetricLongType(metric, "Min"),
        getMetricLongType(metric, "Max"),
        getMetricLongType(metric, "Mean"),
        getMetricLongType(metric, "Count"),
        getMetricLongType(metric, "CountMissValues"),
        getMetricDoubleType(metric, "Var"),
        getMetricDoubleType(metric, "Stddev"),
        getMetricDoubleType(metric, "Sum"),
        getMetricDoubleType(metric, "Skewness"),
        getMetricDoubleType(metric, "Kurtosis"),
        getMetricDoubleType(metric, "Percentile25"),
        getMetricDoubleType(metric, "Median"),
        getMetricDoubleType(metric, "Percentile75"),
        getMetricListStringType(metric, "Category"),
        getMetricCountDistinct(dfStatistics, c),
        getMetricCount(dfStatistics, c, threshold, getMapCategoryCountDiscrete),
        getMetricCount(dfStatistics, c, threshold, getMapCategoryFrequencies),
        getMetricCountMissValuesDiscrete(dfStatistics, c, threshold),
        ingestionTime,
        stageState
      )
    }

    import session.implicits._

    val dataByVariable = listRowByVariable.toDF

    if (storageHandler.exist(path))
      saveDeleteParquet(dataByVariable, path)
    else
      saveParquet(dataByVariable, path)
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
  override def run(): SparkSession = {
    val dataUse: DataFrame = session.read.parquet(datasetPath.toString)
    val attributes: List[String] = schema.discreteAttrs().map(_.name)
    val discreteOps: List[DiscreteMetric] = Metrics.discreteMetrics
    val continuousOps: List[ContinuousMetric] = Metrics.continuousMetrics
    val stageState: String = ???
    val metricsPath: Path = new Path(Settings.comet.metrics.path)

    val discreteDataset =
      Metrics.computeDiscretMetric(dataUse, schema.discreteAttrs().map(_.name), discreteOps)
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
