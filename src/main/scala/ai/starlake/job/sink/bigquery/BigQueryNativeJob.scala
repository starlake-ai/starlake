package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.utils.{JobBase, JobResult, Utils}
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.QueryJobConfiguration.Priority
import com.google.cloud.bigquery._
import com.typesafe.scalalogging.StrictLogging

import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class BigQueryJobResult(tableResult: scala.Option[TableResult]) extends JobResult

class BigQueryNativeJob(
  override val cliConfig: BigQueryLoadConfig,
  sql: String,
  udf: scala.Option[String]
)(implicit val settings: Settings)
    extends JobBase
    with BigQueryJobBase {

  override def name: String = s"bqload-${cliConfig.outputDataset}-${cliConfig.outputTable}"

  override val projectId: String = ServiceOptions.getDefaultProjectId

  logger.info(s"BigQuery Config $cliConfig")

  def runInteractiveQuery(): BigQueryJobResult = {
    val queryConfig: QueryJobConfiguration.Builder =
      QueryJobConfiguration
        .newBuilder(sql)
        .setAllowLargeResults(true)
    logger.info(s"Running BQ Query $sql")
    val queryConfigWithUDF = addUDFToQueryConfig(queryConfig)
    val results =
      BigQueryJobBase.bigquery.query(queryConfigWithUDF.setPriority(Priority.INTERACTIVE).build())
    logger.info(
      s"Query large results performed successfully: ${results.getTotalRows} rows inserted."
    )
    BigQueryJobResult(Some(results))
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
      val results =
        BigQueryJobBase.bigquery.query(queryConfigWithUDF.setDestinationTable(tableId).build())
      logger.info(
        s"Query large results performed successfully: ${results.getTotalRows} rows inserted."
      )

      applyRLSAndCLS().recover { case e =>
        Utils.logException(logger, e)
        throw new Exception(e)
      }

      BigQueryJobResult(Some(results))
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
        BigQueryJobBase.bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build)
      logger.info(
        s"Batch query wth jobId $jobId sent to BigQuery "
      )
      if (job == null)
        throw new Exception("Job not executed since it no longer exists.")
      else
        job
    }
  }
}

object BigQueryNativeJob extends StrictLogging {
  def createTable(datasetName: String, tableName: String, schema: Schema): Unit = {
    Try {
      val tableId = TableId.of(datasetName, tableName)
      val table = scala.Option(BigQueryJobBase.bigquery.getTable(tableId))
      table match {
        case Some(tbl) if tbl.exists() =>
        case _ =>
          val tableDefinition = StandardTableDefinition.of(schema)
          val tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build
          BigQueryJobBase.bigquery.create(tableInfo)
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
      val tableId = BigQueryJobBase.extractProjectDatasetAndTable(key)
      val viewRef = scala.Option(BigQueryJobBase.bigquery.getTable(tableId))
      if (viewRef.isEmpty) {
        logger.info(s"View $tableId does not exist, creating it!")
        BigQueryJobBase.bigquery.create(TableInfo.of(tableId, viewDefinition.build()))
        logger.info(s"View $tableId created")
      } else {
        logger.info(s"View $tableId already exist")
      }
    }
  }
}
