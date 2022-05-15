package ai.starlake.job.metrics

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.metrics.Metrics.{ContinuousMetric, DiscreteMetric, MetricsDatasets}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.Engine.SPARK
import ai.starlake.schema.model.{Domain, Schema, Stage}
import ai.starlake.utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{col, lit}

import scala.util.{Success, Try}

/** To record statistics with other information during ingestion.
  */

/** @param domain
  *   : Domain name
  * @param schema
  *   : Schema
  * @param stage
  *   : stage
  * @param storageHandler
  *   : Storage Handler
  */
class MetricsJob(
  domain: Domain,
  schema: Schema,
  stage: Stage,
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler
)(implicit val settings: Settings)
    extends SparkJob {

  override def name: String = "Compute metrics job"

  /** Function to build the metrics save path
    *
    * @param path
    *   : path where metrics are stored
    * @return
    *   : path where the metrics for the specified schema are stored
    */
  def metricsPath(path: String): Path = {
    DatasetArea.metrics(domain.name, schema.name)
  }

  def lockPath(path: String): Path = {
    new Path(
      settings.comet.lock.path,
      "metrics" + path
        .replace("{domain}", domain.name)
        .replace("{schema}", schema.name)
        .replace('/', '_') + ".lock"
    )
  }

  /** Function Function that unifies discrete and continuous metrics dataframe, then write save the
    * result to parquet
    *
    * @param discreteDataset
    *   : dataframe that contains all the discrete metrics
    * @param continuousDataset
    *   : dataframe that contains all the continuous metrics
    * @param domain
    *   : name of the domain
    * @param schema
    *   : schema of the initial data
    * @param ingestionTime
    *   : time which correspond to the ingestion
    * @param stageState
    *   : stage (unit / global)
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
  ): MetricsDatasets = {
    def computeFrequenciesDF(discreteDataset: DataFrame) = {
      Some(
        discreteDataset
          .select("attribute", "catCountFreq")
          .withColumn("exploded", org.apache.spark.sql.functions.explode(col("catCountFreq")))
          .withColumn("category", col("exploded.category"))
          .withColumn("count", col("exploded.countDiscrete"))
          .withColumn("frequency", col("exploded.frequency"))
          .drop("catCountFreq")
          .drop("exploded")
      )
    }

    val (continuousDF, discreteDF, frequenciesDF) =
      (discreteDataset, continuousDataset) match {
        case (Some(discreteDataset), Some(continuousDataset)) =>
          (
            Some(continuousDataset),
            Some(discreteDataset.drop("catCountFreq")),
            computeFrequenciesDF(discreteDataset)
          )
        case (None, Some(continuousDataset)) =>
          (
            Some(continuousDataset),
            None,
            None
          )
        case (Some(discreteDataset), None) =>
          (
            None,
            Some(discreteDataset.drop("catCountFreq")),
            computeFrequenciesDF(discreteDataset)
          )
        case (None, None) =>
          (
            None,
            None,
            None
          )
      }

    val allDF = List(continuousDF, discreteDF, frequenciesDF).map {
      case Some(dataset) =>
        val res = dataset
          .withColumn("jobId", lit(session.sparkContext.applicationId))
          .withColumn("domain", lit(domain.name))
          .withColumn("schema", lit(schema.name))
          .withColumn("count", lit(count))
          .withColumn("cometTime", lit(ingestionTime))
          .withColumn("cometStage", lit(stageState.toString))
        logger.whenDebugEnabled {
          logger.debug(res.showString())
        }

        Some(res)
      case None => None
    }
    MetricsDatasets(allDF(0), allDF(1), allDF(2))
  }

  /** Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    val datasetPath = new Path(DatasetArea.accepted(domain.name), schema.name)
    val dataUse: DataFrame =
      session.read.format(settings.comet.defaultFormat).load(datasetPath.toString)
    run(dataUse, storageHandler.lastModified(datasetPath))
  }

  def run(dataUse: DataFrame, timestamp: Timestamp): Try[SparkJobResult] = {
    val discAttrs: List[String] = schema.discreteAttrs(schemaHandler).map(_.getFinalName())
    val continAttrs: List[String] = schema.continuousAttrs(schemaHandler).map(_.getFinalName())
    logger.info("Discrete Attributes -> " + discAttrs.mkString(","))
    logger.info("Continuous Attributes -> " + continAttrs.mkString(","))
    val discreteOps: List[DiscreteMetric] = Metrics.discreteMetrics
    val continuousOps: List[ContinuousMetric] = Metrics.continuousMetrics
    val savePath: Path = DatasetArea.metrics(domain.name, schema.name)
    val count = dataUse.count()
    val discreteDataset = Metrics.computeDiscretMetric(dataUse, discAttrs, discreteOps)
    val continuousDataset = Metrics.computeContinuousMetric(dataUse, continAttrs, continuousOps)
    val metricsDatasets =
      unionDisContMetric(
        discreteDataset,
        continuousDataset,
        domain,
        schema,
        count,
        timestamp,
        stage
      )

    val metricsToSave = List(
      (metricsDatasets.continuousDF, MetricsTable.CONTINUOUS),
      (metricsDatasets.discreteDF, MetricsTable.DISCRETE),
      (metricsDatasets.frequenciesDF, MetricsTable.FREQUENCIES)
    )
    val combinedResult = metricsToSave.map { case (df, table) =>
      df match {
        case Some(df) =>
          settings.comet.internal.foreach(in => df.persist(in.cacheStorageLevel))
          new SinkUtils.sink(
            settings.comet.metrics.sink,
            df,
            table.toString,
            new Path(savePath, table.toString),
            lockPath(settings.comet.metrics.path),
            storageHandler,
            SPARK,
            session
          )
        case None =>
          Success(None)
      }
    }
    combinedResult.find(_.isFailure).getOrElse(Success(None)).map(_ => SparkJobResult(None))
  }
}
