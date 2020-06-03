package com.ebiznext.comet.job.metrics

import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.job.index.bqload.{BigQueryLoadConfig, BigQueryLoadJob}
import com.ebiznext.comet.job.index.jdbcload.JdbcLoadConfig
import com.ebiznext.comet.job.ingest.MetricRecord
import com.ebiznext.comet.job.metrics.Metrics.{ContinuousMetric, DiscreteMetric, MetricsDatasets}
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.{Domain, Schema, Stage}
import com.ebiznext.comet.utils.{FileLock, SparkJob, SparkJobResult, Utils}
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{col, lit}

import scala.util.{Success, Try}

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

  /**
    * Saves a dataset. If the path is empty (the first time we call metrics on the schema) then we can write.
    *
    * If there's already parquet files stored in it, then create a temporary directory to compute on, and flush
    * the path to move updated metrics in it
    *
    * @param dataToSave :   dataset to be saved
    * @param path       :   Path to save the file at
    *
    *
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
          .withColumn("jobId", lit(settings.comet.jobId))
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

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): Try[SparkJobResult] = {
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
    val savePath: Path = metricsPath(settings.comet.metrics.path)
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
      (metricsDatasets.continuousDF, "continuous"),
      (metricsDatasets.discreteDF, "discrete"),
      (metricsDatasets.frequenciesDF, "frequencies")
    )
    val combinedResult = metricsToSave.map {
      case (df, name) =>
        df match {
          case Some(df) =>
            settings.comet.internal.foreach(in => df.persist(in.cacheStorageLevel))
            val lockedPath = lockPath(settings.comet.metrics.path)
            val waitTimeMillis = settings.comet.lock.metricsTimeout
            val locker = new FileLock(lockedPath, storageHandler)

            val metricsResult = locker.tryExclusively(waitTimeMillis) {
              save(df, new Path(savePath, name))
            }

            val metricsSinkResult = sinkMetrics(df, name)

            for {
              _ <- metricsResult
              _ <- metricsSinkResult
            } yield {
              SparkJobResult(session)
            }

          case None =>
            Success(SparkJobResult(session))
        }
    }
    combinedResult.find(_.isFailure).getOrElse(Success(SparkJobResult(session)))
  }

  private def sinkMetrics(metricsDf: DataFrame, table: String): Try[Unit] = {
    if (settings.comet.metrics.active) {
      settings.comet.metrics.index match {
        case Settings.IndexSinkSettings.None =>
          Success(())

        case Settings.IndexSinkSettings.BigQuery(bqDataset) =>
          Try {
            sinkMetricsToBigQuery(metricsDf, bqDataset, table)
          }

        case Settings.IndexSinkSettings.Jdbc(jdbcConnection, partitions, batchSize) =>
          Try {
            val jdbcConfig = JdbcLoadConfig.fromComet(
              jdbcConnection,
              settings.comet,
              Right(metricsDf),
              name,
              partitions = partitions,
              batchSize = batchSize
            )
            sinkMetricsToJdbc(jdbcConfig)
          }
      }
    } else {
      Success(())
    }
  }

  private def sinkMetricsToBigQuery(
    metricsDf: DataFrame,
    bqDataset: String,
    bqTable: String
  ): Unit = {
    if (metricsDf.count() > 0) {
      val config = BigQueryLoadConfig(
        Right(metricsDf),
        outputDataset = bqDataset,
        outputTable = bqTable,
        None,
        "parquet",
        "CREATE_IF_NEEDED",
        "WRITE_APPEND",
        None,
        None
      )
      // Do not pass the schema here. Not that we do not compute the schema correctly
      // But since we are having a record of repeated field BQ does not like
      // the way we pass the schema. BQ needs an extra "list" subfield for repeated fields
      // So let him determine teh schema by himself or risk tonot to be able to append the metrics
      val res = new BigQueryLoadJob(config).run()
      Utils.logFailure(res, logger)
    }
  }

  private implicit val memsideEncoder: Encoder[MetricRecord] = Encoders.product[MetricRecord]

  private implicit val sqlableEncoder: Encoder[MetricRecord.AsSql] =
    Encoders.product[MetricRecord.AsSql]

  private def sinkMetricsToJdbc(
    cliConfig: JdbcLoadConfig
  ): Unit = {
    cliConfig.sourceFile match {
      case Left(_) =>
        throw new IllegalArgumentException("unsupported case with named source")
      case Right(metricsDf) =>
        // TODO: SMELL: Refused Bequest
        require(
          cliConfig.writeDisposition == WriteDisposition.WRITE_APPEND,
          s"unsupported write disposition ${cliConfig.writeDisposition}, only WRITE_APPEND is supported"
        )

        val converter = MetricRecord.MetricRecordConverter()

        val sqlableMetricsDf = metricsDf.as[MetricRecord].map(converter.toSqlCompatible)

        sqlableMetricsDf.write
          .format("jdbc")
          .option("numPartitions", cliConfig.partitions)
          .option("batchsize", cliConfig.batchSize)
          .option("truncate", cliConfig.writeDisposition == WriteDisposition.WRITE_TRUNCATE)
          .option("driver", cliConfig.driver)
          .option("url", cliConfig.url)
          .option("dbtable", cliConfig.outputTable)
          .option("user", cliConfig.user)
          .option("password", cliConfig.password)
          .mode(SaveMode.Append)
          .save()
    }
  }

}
