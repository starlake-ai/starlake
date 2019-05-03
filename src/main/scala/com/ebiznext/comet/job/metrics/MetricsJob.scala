package com.ebiznext.comet.job.metrics

import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.job.metrics.Metrics.{logger, ContinuousMetric, DiscreteMetric}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.{Domain, Schema, Stage}
import com.ebiznext.comet.utils.SparkJob
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{col, lit}

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

  /** Function that retrieves full metrics dataframe with both set discrete and continuous metrics
    *
    * @param dataMetric : dataframe obtain from computeDiscretMetric( ) or computeContinuiousMetric( )
    * @param listAttibutes : list of all variables
    * @param colName  : list of column
    * @return Dataframe : that contain the full metrics  with all variables and all metrics
    */

  def generateFullMetric(
    dataMetric: DataFrame,
    listAttibutes: List[String],
    colName: List[Column]
  ): DataFrame = {
    listAttibutes
      .foldLeft(dataMetric) { (data, nameCol) =>
        data.withColumn(nameCol, lit(null))
      }
      .select(colName: _*)

  }

  /** Function Function that unifies discrete and continuous metrics dataframe, then write save the result to parquet
    *
    * @param discreteDataset : dataframe that contains all the discrete metrics
    * @param continuousDataset : dataframe that contains all the continuous metrics
    * @param domain    : name of the domain
    * @param schema    : schema of the initial data
    * @param ingestionTime : time which correspond to the ingestion
    * @param stageState   : stage (unit / global)
    * @return
    */

  def unionDisContMetric(
    discreteDataset: DataFrame,
    continuousDataset: DataFrame,
    domain: Domain,
    schema: Schema,
    ingestionTime: Timestamp,
    stageState: Stage
  ): DataFrame = {

    val listDiscAttrName: List[String] = List(
      "min",
      "max",
      "mean",
      "count",
      "variance",
      "standardDev",
      "sum",
      "skewness",
      "kurtosis",
      "percentile25",
      "median",
      "percentile75",
      "missingValues"
    )
    val listContAttrName: List[String] =
      List("category", "countDistinct", "countByCategory", "frequencies", "missingValuesDiscrete")
    val listtotal: List[String] = List(
      "variableName",
      "min",
      "max",
      "mean",
      "count",
      "variance",
      "standardDev",
      "sum",
      "skewness",
      "kurtosis",
      "percentile25",
      "median",
      "percentile75",
      "missingValues",
      "category",
      "countDistinct",
      "countByCategory",
      "frequencies",
      "missingValuesDiscrete"
    )
    val sortSelectCol: List[String] = List(
      "domain",
      "schema",
      "variableName",
      "min",
      "max",
      "mean",
      "count",
      "missingValues",
      "standardDev",
      "variance",
      "sum",
      "skewness",
      "kurtosis",
      "percentile25",
      "median",
      "percentile75",
      "category",
      "countDistinct",
      "countByCategory",
      "frequencies",
      "missingValuesDiscrete",
      "ingestionTime",
      "stageState"
    )

    val neededColList: List[Column] = listtotal.map(x => col(x))

    logger.info(
      "The liste of Columne: " + neededColList
    )
    val coupleDataMetrics =
      List((discreteDataset, listDiscAttrName), (continuousDataset, listContAttrName))

    coupleDataMetrics
      .map(
        tupleDataMetric => generateFullMetric(tupleDataMetric._1, tupleDataMetric._2, neededColList)
      )
      .reduce(_ union _)
      .withColumn("domain", lit(domain.name))
      .withColumn("schema", lit(schema.name))
      .withColumn("ingestionTime", lit(ingestionTime))
      .withColumn("stageState", lit(stageState))
      .select(sortSelectCol.head, sortSelectCol.tail: _*)

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

    val discreteDataset: DataFrame = Metrics.computeDiscretMetric(dataUse, discAttrs, discreteOps)
    val continuousDataset: DataFrame =
      Metrics.computeContinuousMetric(dataUse, continAttrs, continuousOps)

    logger.info("The discret metrics: \n\n ")

    discreteDataset.show()

    logger.info("The Continuous metrics metrics: \n\n ")

    continuousDataset.show()

    logger.info("First's part done: \n\n ")

    val allMetricsDf: DataFrame =
      unionDisContMetric(discreteDataset, continuousDataset, domain, schema, timestamp, stage)

    allMetricsDf.show()

    save(allMetricsDf, savePath)
    session
  }
}
