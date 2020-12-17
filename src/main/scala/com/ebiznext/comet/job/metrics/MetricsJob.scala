package com.ebiznext.comet.job.metrics

import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.job.metrics.Metrics.{ContinuousMetric, DiscreteMetric, MetricsDatasets}
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{col, lit}

import scala.util.{Success, Try}

/** To record statistics with other information during ingestion.
  */

/** @param domain         : Domain name
  * @param schema         : Schema
  * @param stage          : stage
  * @param storageHandler : Storage Handler
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
    * @param path : path where metrics are stored
    * @return : path where the metrics for the specified schema are stored
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
  ): MetricsDatasets = {
    val (continuousDF, discreteDF, frequenciesDF) =
      (discreteDataset, continuousDataset) match {
        case (Some(discreteDataset), Some(continuousDataset)) =>
          (
            Some(continuousDataset),
            Some(discreteDataset.drop("catCountFreq")),
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
            Some(
              discreteDataset
                .select("catCountFreq")
                .withColumn("exploded", org.apache.spark.sql.functions.explode(col("catCountFreq")))
                .withColumn("category", col("exploded.category"))
                .withColumn("count", col("exploded.countDiscrete"))
                .withColumn("frequency", col("exploded.frequency"))
                .drop("catCountFreq")
                .drop("exploded")
            )
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
          res.show()
        }

        Some(res)
      case None => None
    }
    MetricsDatasets(allDF(0), allDF(1), allDF(2))
  }

  /** Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    val datasetPath = new Path(DatasetArea.accepted(domain.name), schema.name)
    val dataUse: DataFrame = session.read.parquet(datasetPath.toString)
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
          val lockedPath = lockPath(settings.comet.metrics.path)
          val waitTimeMillis = settings.comet.lock.metricsTimeout
          val locker = new FileLock(lockedPath, storageHandler)
          val metricsResult = locker.tryExclusively(waitTimeMillis) {
            appendToFile(storageHandler, df, new Path(savePath, table.toString))
          }
          val metricsSinkResult =
            new SinkUtils().sink(settings.comet.metrics.sink, df, table.toString)
          for {
            _ <- metricsResult
            _ <- metricsSinkResult
          } yield {
            None
          }
        case None =>
          Success(None)
      }
    }
    combinedResult.find(_.isFailure).getOrElse(Success(None)).map(SparkJobResult(_))
  }
}
