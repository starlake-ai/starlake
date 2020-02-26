package com.ebiznext.comet.job.metrics

import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.job.metrics.Metrics.{ContinuousMetric, DiscreteMetric}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.{Domain, Schema, Stage}
import com.ebiznext.comet.utils.{FileLock, SparkJob}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}

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
)(implicit val settings: Settings)
    extends SparkJob {

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

  def getLockPath(path: String): Path = {
    new Path(
      settings.comet.lock.path,
      "metrics" + path
        .replace("{domain}", domain.name)
        .replace("{schema}", schema.name)
        .replace('/', '_') + ".lock"
    )
  }

  /**
    * Saves a dataset. If the path is empty (the first time we call metrics on the schema) then we can write.
    *
    * If there's already parquet files stored in it, then create a temporary directory to compute on, and flush
    * the path to move updated metrics in it
    *
    * @param dataToSave :   dataset to be saved
    * @param path       :   Path to save the file at
    */
  def save(dataToSave: DataFrame, path: Path): Unit = {
    if (storageHandler.exists(path)) {
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
    * @param dataMetric    : dataframe obtain from computeDiscretMetric( ) or computeContinuiousMetric( )
    * @param listAttibutes : list of all variables
    * @param colName       : list of column
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
    * @param discreteDataset   : dataframe that contains all the discrete metrics
    * @param continuousDataset : dataframe that contains all the continuous metrics
    * @param domain            : name of the domain
    * @param schema            : schema of the initial data
    * @param ingestionTime     : time which correspond to the ingestion
    * @param stageState        : stage (unit / global)
    * @return
    */

  def unionDisContMetric(
    discreteDataset: Option[DataFrame],
    continuousDataset: Option[DataFrame],
    domain: Domain,
    schema: Schema,
    count: Long,
    ingestionTime: Timestamp,
    stageState: Stage
  ): Option[DataFrame] = {

    val listContAttrName: List[String] = List(
      "min",
      "max",
      "mean",
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
    /*
root
 |-- attribute: string (nullable = false)
 |-- min: long (nullable = true)
 |-- max: long (nullable = true)
 |-- mean: double (nullable = true)
 |-- count: long (nullable = true)
 |-- missingValues: long (nullable = true)
 |-- variance: double (nullable = true)
 |-- standardDev: double (nullable = true)
 |-- sum: long (nullable = true)
 |-- skewness: double (nullable = true)
 |-- kurtosis: double (nullable = true)
 |-- percentile25: long (nullable = true)
 |-- median: long (nullable = true)
 |-- percentile75: long (nullable = true)
 |-- cometMetric: string (nullable = false)

     */
    val continuousSchema = StructType(
      Array(
        StructField("attribute", StringType, false),
        StructField("min", LongType, false),
        StructField("max", LongType, false),
        StructField("mean", DoubleType, false),
        StructField("missingValues", LongType, false),
        StructField("variance", DoubleType, false),
        StructField("standardDev", DoubleType, false),
        StructField("sum", LongType, false),
        StructField("skewness", DoubleType, false),
        StructField("kurtosis", LongType, false),
        StructField("percentile25", LongType, false),
        StructField("median", LongType, false),
        StructField("percentile75", LongType, false),
        StructField("cometMetric", StringType, false)
      )
    )

    val listDiscAttrName: List[String] =
      List("category", "countDistinct", "countByCategory", "frequencies", "missingValuesDiscrete")
    val discreteSchema = StructType(
      Array(
        StructField("attribute", StringType, false),
        StructField("category", ArrayType(StringType), false),
        StructField("countDistinct", LongType, false),
        StructField("countByCategory", ArrayType(MapType(StringType, LongType)), false),
        StructField("frequencies", ArrayType(MapType(StringType, DoubleType)), false),
        StructField("missingValuesDiscrete", LongType, false),
        StructField("cometMetric", StringType, false)
      )
    )

    val listtotal: List[String] = List(
      "attribute",
      "min",
      "max",
      "mean",
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
      "attribute",
      "min",
      "max",
      "mean",
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
      "count",
      "cometTime",
      "cometStage"
    )

    val neededColList: List[Column] = listtotal.map(x => col(x))

    logger.info(
      "The list of Columns: " + neededColList
    )
    /*
    val x = (discreteDataset, continuousDataset) match {

      case (Some(discreteDataset), Some(continuousDataset)) =>
        (discreteDataset, continuousDataset)
      case (Some(discreteDataset), None) =>
        (discreteDataset, )
      case (None, Some(continuousDataset)) =>
        Some(continuousDataset)
      case (None, None) =>
        None
    }
     */
    discreteDataset.foreach(_.printSchema())
    continuousDataset.foreach(_.printSchema())

    val emptyContinuousDataset =
      session.createDataFrame(new java.util.ArrayList[Row](), continuousSchema)
    val emptyDiscreteDataset =
      session.createDataFrame(new java.util.ArrayList[Row](), discreteSchema)

    val coupleDataMetrics =
      (discreteDataset, continuousDataset) match {
        case (Some(discreteDataset), Some(continuousDataset)) =>
          List((discreteDataset, listContAttrName), (continuousDataset, listDiscAttrName))
        case (None, Some(continuousDataset)) =>
          List((emptyDiscreteDataset, listContAttrName), (continuousDataset, listDiscAttrName))
        case (Some(discreteDataset), None) =>
          List((discreteDataset, listContAttrName), (emptyContinuousDataset, listDiscAttrName))
        case (None, None) =>
          List((emptyDiscreteDataset, listContAttrName), (emptyContinuousDataset, listDiscAttrName))
      }

    val result = coupleDataMetrics
      .map(
        tupleDataMetric => generateFullMetric(tupleDataMetric._1, tupleDataMetric._2, neededColList)
      )
      .reduce(_ union _)
      .withColumn("domain", lit(domain.name))
      .withColumn("schema", lit(schema.name))
      .withColumn("count", lit(count))
      .withColumn("cometTime", lit(ingestionTime))
      .withColumn("cometStage", lit(stageState.toString))
      .select(sortSelectCol.head, sortSelectCol.tail: _*)
    Some(result)
  }

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): Try[SparkSession] = {
    val datasetPath = new Path(DatasetArea.accepted(domain.name), schema.name)
    val dataUse: DataFrame = session.read.parquet(datasetPath.toString)
    run(dataUse, storageHandler.lastModified(datasetPath))
  }

  def run(dataUse: DataFrame, timestamp: Timestamp): Try[SparkSession] = {
    val discAttrs: List[String] = schema.discreteAttrs().map(_.getFinalName())
    val continAttrs: List[String] = schema.continuousAttrs().map(_.getFinalName())
    logger.info("Discrete Attributes -> " + discAttrs.mkString(","))
    logger.info("Continuous Attributes -> " + continAttrs.mkString(","))
    val discreteOps: List[DiscreteMetric] = Metrics.discreteMetrics
    val continuousOps: List[ContinuousMetric] = Metrics.continuousMetrics
    val savePath: Path = getMetricsPath(settings.comet.metrics.path)
    val count = dataUse.count()
    val discreteDataset = Metrics.computeDiscretMetric(dataUse, discAttrs, discreteOps)
    val continuousDataset = Metrics.computeContinuousMetric(dataUse, continAttrs, continuousOps)
    val allMetricsDfMaybe =
      unionDisContMetric(
        discreteDataset,
        continuousDataset,
        domain,
        schema,
        count,
        timestamp,
        stage
      )

    val metricsResult = allMetricsDfMaybe match {
      case Some(allMetricsDf) =>
        val lockPath = getLockPath(settings.comet.metrics.path)
        val waitTimeMillis = settings.comet.lock.metricsTimeout
        val locker = new FileLock(lockPath, storageHandler)

        locker.tryExclusively(waitTimeMillis) {
          save(allMetricsDf, savePath)
        }

      case None =>
        Success(())
    }

    metricsResult.map(_ => session)
  }
}
