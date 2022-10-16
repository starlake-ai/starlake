package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.utils.{JobBase, JobResult, TableFormatter, Utils}
import better.files.File
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import com.google.cloud.bigquery.QueryJobConfiguration.Priority
import com.google.cloud.bigquery._
import com.google.gson.Gson
import com.typesafe.scalalogging.StrictLogging

import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class BigQueryJobResult(tableResult: scala.Option[TableResult], totalBytesProcessed: Long)
    extends JobResult {

  def show(format: String, rootServe: scala.Option[String]): Unit = {
    val output = rootServe.map(File(_, "run.log"))
    output.foreach(_.overwrite(s"Total Bytes Processed: $totalBytesProcessed bytes.\n"))
    println(s"Total Bytes Processed: $totalBytesProcessed bytes.")
    tableResult.foreach { rows =>
      val headers = rows.getSchema.getFields.iterator().asScala.toList.map(_.getName)
      val values =
        rows.getValues.iterator().asScala.toList.map { row =>
          row
            .iterator()
            .asScala
            .toList
            .map(cell => scala.Option(cell.getValue()).getOrElse("null").toString)
        }

      format match {
        case "csv" =>
          (headers :: values).foreach { row =>
            output.foreach(_.appendLine(row.mkString(",")))
            println(row.mkString(","))
          }

        case "table" =>
          headers :: values match {
            case Nil =>
              output.foreach(_.appendLine("Result is empty."))
              println("Result is empty.")
            case _ =>
              output.foreach(_.appendLine(TableFormatter.format(headers :: values)))
              println(TableFormatter.format(headers :: values))
          }

        case "json" =>
          values.foreach { value =>
            val map = headers.zip(value).toMap
            val json = new Gson().toJson(map.asJava)
            output.foreach(_.appendLine(json))
            println(json)
          }
      }
    }
  }
}

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

  def runInteractiveQuery(): Try[JobResult] = {
    Try {
      val queryConfig: QueryJobConfiguration.Builder =
        QueryJobConfiguration
          .newBuilder(sql)
          .setAllowLargeResults(true)
      logger.info(s"Running interactive BQ Query $sql")
      val queryConfigWithUDF = addUDFToQueryConfig(queryConfig)
      val finalConfiguration = queryConfigWithUDF.setPriority(Priority.INTERACTIVE).build()

      val queryJob = BigQueryJobBase.bigquery.create(JobInfo.of(finalConfiguration))
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
      val jobInfo = BigQueryJobBase.bigquery.create(JobInfo.of(finalConfiguration))
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
