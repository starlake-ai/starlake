package ai.starlake.job.metrics

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.metrics.Metrics.{ContinuousMetric, DiscreteMetric, MetricsDatasets}
import ai.starlake.job.transform.SparkAutoTask
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{AutoTaskDesc, Domain, Schema}
import ai.starlake.utils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{col, lit}

import scala.util.{Failure, Success, Try}

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
  appId: Option[String],
  domain: Domain,
  schema: Schema,
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler
)(implicit val settings: Settings)
    extends SparkJob {

  override def name: String = "Compute metrics job"

  override def applicationId(): String = appId.getOrElse(super.applicationId())

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
      settings.appConfig.lock.path,
      "metrics" + path
        .replace("{{domain}}", domain.name)
        .replace("{{schema}}", schema.name)
        .replace(":", "_")
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

  private def unionDisContMetric(
    discreteDataset: Option[DataFrame],
    continuousDataset: Option[DataFrame],
    domain: Domain,
    schema: Schema,
    count: Long,
    ingestionTime: Timestamp
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
          .withColumn("jobId", lit(applicationId()))
          .withColumn("domain", lit(domain.name))
          .withColumn("schema", lit(schema.name))
          .withColumn("count", lit(count))
          .withColumn("timestamp", lit(ingestionTime))
        logger.whenDebugEnabled {
          logger.debug(res.showString())
        }

        Some(res)
      case None => None
    }
    MetricsDatasets(allDF(0), allDF(1), allDF(2))
  }

  /** Just to force any job to implement its entry point using within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    val dataUse: DataFrame = session.sql(s"SELECT * FROM ${domain.name}.${schema.name}")
    val now = System.currentTimeMillis()
    run(dataUse, now)
  }

  def run(dataUse: DataFrame, timestamp: Timestamp): Try[SparkJobResult] = {
    val discAttrs: List[String] = schema.discreteAttrs(schemaHandler).map(_.getFinalName())
    val continAttrs: List[String] = schema.continuousAttrs(schemaHandler).map(_.getFinalName())
    logger.info("Discrete Attributes -> " + discAttrs.mkString(","))
    logger.info("Continuous Attributes -> " + continAttrs.mkString(","))
    val discreteOps: List[DiscreteMetric] = Metrics.discreteMetrics
    val continuousOps: List[ContinuousMetric] = Metrics.continuousMetrics
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
        timestamp
      )
    val metricsToSave = List(
      (metricsDatasets.continuousDF, MetricsTable.CONTINUOUS),
      (metricsDatasets.discreteDF, MetricsTable.DISCRETE),
      (metricsDatasets.frequenciesDF, MetricsTable.FREQUENCIES)
    )
    val combinedResult = metricsToSave.map { case (df, table) =>
      df match {
        case Some(df) =>
          settings.appConfig.internal.foreach(in => df.persist(in.cacheStorageLevel))
          val taskDesc =
            AutoTaskDesc(
              name = applicationId(),
              sql = None,
              database = settings.appConfig.audit.getDatabase(),
              domain = settings.appConfig.audit.getDomain(),
              table = table.toString,
              presql = Nil,
              postsql = Nil,
              sink = Some(settings.appConfig.audit.sink),
              parseSQL = Some(true),
              _auditTableName = Some(table.toString),
              connectionRef = settings.appConfig.audit.sink.connectionRef
            )
          val autoTask = new SparkAutoTask(
            appId = Option(applicationId()),
            taskDesc = taskDesc,
            commandParameters = Map.empty,
            interactive = None,
            truncate = false,
            test = false,
            logExecution = false,
            resultPageSize = 200,
            resultPageNumber = 1
          )(
            settings,
            storageHandler,
            schemaHandler
          )
          autoTask.sink(df)
        case None =>
          true
      }
    }
    val success = combinedResult.find(_ == false).getOrElse(true)
    if (success) {
      Success(SparkJobResult(None, None))
    } else {
      Failure(new Exception("Failed to save metrics"))
    }
  }
}
