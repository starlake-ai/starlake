package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.utils.{JobBase, JobResult, Utils}
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import com.google.cloud.bigquery.QueryJobConfiguration.Priority
import com.google.cloud.bigquery._
import com.typesafe.scalalogging.StrictLogging

import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class BigQueryNativeJob(
  override val cliConfig: BigQueryLoadConfig,
  sql: String,
  udf: scala.Option[String]
)(implicit val settings: Settings)
    extends JobBase
    with BigQueryJobBase {

  override def name: String = s"bqload-${cliConfig.outputDataset}-${cliConfig.outputTable}"

  override def projectId: String =
    cliConfig.gcpProjectId.getOrElse(ServiceOptions.getDefaultProjectId())

  logger.info(s"BigQuery Config $cliConfig")

  def runInteractiveQuery(): Try[JobResult] = {
    Try {
      val queryConfig: QueryJobConfiguration.Builder =
        QueryJobConfiguration
          .newBuilder(sql)
          .setAllowLargeResults(true)
      logger.info(s"Running interactive BQ Query $sql")
      val queryConfigWithUDF = addUDFToQueryConfig(queryConfig)
      val finalConfiguration = queryConfigWithUDF.setPriority(Priority.INTERACTIVE).build()

      val queryJob = bigquery().create(JobInfo.of(finalConfiguration))
      val totalBytesProcessed = queryJob
        .getStatistics()
        .asInstanceOf[QueryStatistics]
        .getTotalBytesProcessed

      val results = queryJob.getQueryResults()
      logger.info(
        s"Query large results performed successfully: ${results.getTotalRows} rows returned."
      )

      BigQueryJobResult(Some(results), totalBytesProcessed)
    }
  }

  private def addUDFToQueryConfig(
    queryConfig: QueryJobConfiguration.Builder
  ): QueryJobConfiguration.Builder = {
    val queryConfigWithUDF = udf
      .map { udf =>
        queryConfig.setUserDefinedFunctions(List(UserDefinedFunction.fromUri(udf)).asJava)
      }
      .getOrElse(queryConfig)
    queryConfigWithUDF
  }

  /** Just to force any spark job to implement its entry point within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    if (cliConfig.materializedView) {
      RunAndSinkAsMaterializedView().map(table => BigQueryJobResult(None, 0L))
    } else {
      RunAndSinkAsTable()
    }
  }
  private def RunAndSinkAsMaterializedView(): Try[Table] = {
    Try {
      val materializedViewDefinitionBuilder = MaterializedViewDefinition.newBuilder(sql)
      cliConfig.outputPartition match {
        case Some(partitionField) =>
          // Generating schema from YML to get the descriptions in BQ
          val partitioning =
            timePartitioning(partitionField, cliConfig.days, cliConfig.requirePartitionFilter)
              .build()
          materializedViewDefinitionBuilder.setTimePartitioning(partitioning)
        case None =>
      }
      cliConfig.outputClustering match {
        case Nil =>
        case fields =>
          val clustering = Clustering.newBuilder().setFields(fields.asJava).build()
          materializedViewDefinitionBuilder.setClustering(clustering)
      }
      cliConfig.options.get("enableRefresh") match {
        case Some(x) => materializedViewDefinitionBuilder.setEnableRefresh(x.toBoolean)
        case None    =>
      }
      cliConfig.options.get("refreshIntervalMs") match {
        case Some(x) => materializedViewDefinitionBuilder.setRefreshIntervalMs(x.toLong)
        case None    =>
      }
      bigquery().create(TableInfo.of(tableId, materializedViewDefinitionBuilder.build()))
    }
  }

  private def RunAndSinkAsTable(): Try[BigQueryJobResult] = {
    Try {
      val targetDataset = getOrCreateDataset()
      val queryConfig: QueryJobConfiguration.Builder =
        QueryJobConfiguration
          .newBuilder(sql)
          .setCreateDisposition(CreateDisposition.valueOf(cliConfig.createDisposition))
          .setWriteDisposition(WriteDisposition.valueOf(cliConfig.writeDisposition))
          .setDefaultDataset(targetDataset.getDatasetId)
          .setPriority(Priority.INTERACTIVE)
          .setUseLegacySql(false)
          .setAllowLargeResults(true)

      logger.info("Computing partitionning")
      val queryConfigWithPartition = cliConfig.outputPartition match {
        case Some(partitionField) =>
          // Generating schema from YML to get the descriptions in BQ
          val partitioning =
            timePartitioning(partitionField, cliConfig.days, cliConfig.requirePartitionFilter)
              .build()
          queryConfig.setTimePartitioning(partitioning)
        case None =>
          queryConfig
      }

      logger.info("Computing clustering")
      val queryConfigWithClustering = cliConfig.outputClustering match {
        case Nil =>
          queryConfigWithPartition
        case fields =>
          val clustering = Clustering.newBuilder().setFields(fields.asJava).build()
          queryConfigWithPartition.setClustering(clustering)
      }
      logger.info("Add user defined functions")
      val queryConfigWithUDF = addUDFToQueryConfig(queryConfigWithClustering)
      logger.info(s"Executing BQ Query $sql")
      val finalConfiguration = queryConfigWithUDF.setDestinationTable(tableId).build()
      val jobInfo = bigquery().create(JobInfo.of(finalConfiguration))
      val totalBytesProcessed = jobInfo
        .getStatistics()
        .asInstanceOf[QueryStatistics]
        .getTotalBytesProcessed

      val results = jobInfo.getQueryResults()
      logger.info(
        s"Query large results performed successfully: ${results.getTotalRows} rows inserted."
      )

      applyRLSAndCLS().recover { case e =>
        Utils.logException(logger, e)
        throw new Exception(e)
      }

      BigQueryJobResult(Some(results), totalBytesProcessed)
    }
  }

  def runBatchQuery(): Try[Job] = {
    Try {
      getOrCreateDataset()
      val jobId = JobId
        .newBuilder()
        .setJob(
          UUID.randomUUID.toString
        ) // Run at batch priority, which won't count toward concurrent rate limit.
        .setLocation(cliConfig.getLocation())
        .build()
      val queryConfig =
        QueryJobConfiguration
          .newBuilder(sql)
          .setPriority(Priority.BATCH)
          .setUseLegacySql(false)
          .build()
      logger.info(s"Executing BQ Query $sql")
      val job =
        bigquery().create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build)
      logger.info(
        s"Batch query wth jobId $jobId sent to BigQuery "
      )
      if (job == null)
        throw new Exception("Job not executed since it no longer exists.")
      else
        job
    }
  }

  def createTable(datasetName: String, tableName: String, schema: Schema): Unit = {
    Try {
      val tableId = TableId.of(datasetName, tableName)
      val table = scala.Option(bigquery().getTable(tableId))
      table match {
        case Some(tbl) if tbl.exists() =>
        case _ =>
          val tableDefinition = StandardTableDefinition.of(schema)
          val tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build
          bigquery().create(tableInfo)
          logger.info(s"Table $datasetName.$tableName created successfully")
      }
    } match {
      case Success(_) =>
      case Failure(e) =>
        logger.info(s"Table $datasetName.$tableName was not created.")
        Utils.logException(logger, e)
    }
  }

  @deprecated("Views are now created using the syntax WTH ... AS ...", "0.1.25")
  def createViews(views: Map[String, String], udf: scala.Option[String]) = {
    views.foreach { case (key, value) =>
      val viewQuery: ViewDefinition.Builder =
        ViewDefinition.newBuilder(value).setUseLegacySql(false)
      val viewDefinition = udf
        .map { udf =>
          viewQuery
            .setUserDefinedFunctions(List(UserDefinedFunction.fromUri(udf)).asJava)
        }
        .getOrElse(viewQuery)
      val tableId = extractProjectDatasetAndTable(key)
      val viewRef = scala.Option(bigquery().getTable(tableId))
      if (viewRef.isEmpty) {
        logger.info(s"View $tableId does not exist, creating it!")
        bigquery().create(TableInfo.of(tableId, viewDefinition.build()))
        logger.info(s"View $tableId created")
      } else {
        logger.info(s"View $tableId already exist")
      }
    }
  }
}

object BigQueryNativeJob extends StrictLogging {}
