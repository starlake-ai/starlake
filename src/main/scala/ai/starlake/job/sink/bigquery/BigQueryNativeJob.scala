package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.job.ingest.{AuditLog, Step}
import ai.starlake.schema.model.{BigQuerySink, Format}
import ai.starlake.utils.{JobBase, JobResult, Utils}
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, SchemaUpdateOption, WriteDisposition}
import com.google.cloud.bigquery.JobStatistics.{LoadStatistics, QueryStatistics}
import com.google.cloud.bigquery.QueryJobConfiguration.Priority
import com.google.cloud.bigquery.{Schema => BQSchema, Table, _}
import com.typesafe.scalalogging.StrictLogging

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.Try

class BigQueryNativeJob(
  override val cliConfig: BigQueryLoadConfig,
  sql: String,
  udf: scala.Option[String]
)(implicit val settings: Settings)
    extends JobBase
    with BigQueryJobBase {
  override def name: String = s"bqload-${bqNativeTable}"

  logger.info(s"BigQuery Config $cliConfig")

  def loadPathsToBQ(bqSchema: BQSchema): Try[BigQueryJobResult] = {
    getOrCreateDataset(cliConfig.domainDescription).flatMap { _ =>
      Try {
        logger.info(s"BigQuery Schema: $bqSchema")
        val formatOptions: FormatOptions = bqFormatOptions()
        cliConfig.source match {
          case Left(sourceURIs) =>
            val loadConfig: LoadJobConfiguration.Builder =
              bqLoadConfig(bqSchema, formatOptions, sourceURIs)
            // Load data from a GCS CSV file into the table
            val job = bigquery().create(JobInfo.of(loadConfig.build))
            // Blocks until this load table job completes its execution, either failing or succeeding.
            val jobResult = job.waitFor()
            if (jobResult.isDone) {
              val stats = jobResult.getStatistics.asInstanceOf[LoadStatistics]
              applyRLSAndCLS().recover { case e =>
                Utils.logException(logger, e)
                throw e
              }
              logger.info(
                s"bq-ingestion-summary -> files: [$sourceURIs], domain: ${tableId.getDataset}, schema: ${tableId.getTable}, input: ${stats.getOutputRows + stats.getBadRecords}, accepted: ${stats.getOutputRows}, rejected:${stats.getBadRecords}"
              )
              val success = !settings.comet.rejectAllOnError || stats.getBadRecords == 0
              val log = AuditLog(
                jobResult.getJobId.getJob,
                sourceURIs,
                BigQueryJobBase.getBqNativeDataset(tableId),
                tableId.getTable,
                success = success,
                stats.getOutputRows + stats.getBadRecords,
                stats.getOutputRows,
                stats.getBadRecords,
                Timestamp.from(Instant.ofEpochMilli(stats.getStartTime)),
                stats.getEndTime - stats.getStartTime,
                if (success) "success" else s"${stats.getBadRecords} invalid records",
                Step.LOAD.toString,
                settings.comet.database,
                settings.comet.tenant
              )
              settings.comet.audit.sink match {
                case sink: BigQuerySink =>
                  AuditLog.sinkToBigQuery(Map.empty, log, sink)
                case _ =>
                  throw new Exception("Not Supported")
              }
              BigQueryJobResult(None, stats.getInputBytes)
            } else
              throw new Exception(
                "BigQuery was unable to load into the table due to an error:" + jobResult.getStatus.getError
              )
          case Right(_) =>
            throw new Exception("Should never happen")

        }
      }
    }
  }

  private def bqLoadConfig(
    bqSchema: BQSchema,
    formatOptions: FormatOptions,
    sourceURIs: String
  ): LoadJobConfiguration.Builder = {
    val loadConfig =
      LoadJobConfiguration
        .newBuilder(tableId, sourceURIs.split(",").toList.asJava, formatOptions)
        .setIgnoreUnknownValues(true)
        .setCreateDisposition(JobInfo.CreateDisposition.valueOf(cliConfig.createDisposition))
        .setWriteDisposition(JobInfo.WriteDisposition.valueOf(cliConfig.writeDisposition))
        .setSchema(bqSchema)

    if (cliConfig.writeDisposition == JobInfo.WriteDisposition.WRITE_APPEND.toString) {
      loadConfig.setSchemaUpdateOptions(
        List(
          SchemaUpdateOption.ALLOW_FIELD_ADDITION,
          SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ).asJava
      )
      if (!settings.comet.rejectAllOnError)
        loadConfig.setMaxBadRecords(settings.comet.rejectMaxRecords)
    }

    cliConfig.outputPartition match {
      case Some(partitionField) =>
        // Generating schema from YML to get the descriptions in BQ
        val partitioning =
          timePartitioning(partitionField, cliConfig.days, cliConfig.requirePartitionFilter)
            .build()
        loadConfig.setTimePartitioning(partitioning)
      case None =>
    }
    cliConfig.outputClustering match {
      case Nil =>
      case fields =>
        val clustering = Clustering.newBuilder().setFields(fields.asJava).build()
        loadConfig.setClustering(clustering)
    }
    loadConfig
  }

  private def bqFormatOptions(): FormatOptions = {
    val formatOptions = cliConfig.starlakeSchema.flatMap(_.metadata) match {
      case Some(metadata) =>
        metadata.getFormat() match {
          case Format.DSV =>
            val formatOptions =
              CsvOptions.newBuilder.setAllowQuotedNewLines(true).setAllowJaggedRows(true)
            if (metadata.isWithHeader())
              formatOptions.setSkipLeadingRows(1).build
            metadata.encoding match {
              case Some(encoding) =>
                formatOptions.setEncoding(encoding)
              case None =>
            }
            formatOptions.setFieldDelimiter(metadata.getSeparator())
            metadata.quote.map(quote => formatOptions.setQuote(quote))

            formatOptions.build()
          case Format.JSON =>
            FormatOptions.json()
          case _ =>
            throw new Exception("Should never happen")
        }
      case None =>
        throw new Exception("Should never happen")
    }
    formatOptions
  }

  def runInteractiveQuery(): Try[JobResult] = {
    getOrCreateDataset(cliConfig.domainDescription).flatMap { _ =>
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
    getOrCreateDataset(None).flatMap { _ =>
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
  }

  private def RunAndSinkAsTable(): Try[BigQueryJobResult] = {
    getOrCreateDataset(None).flatMap { targetDataset =>
      Try {
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

            // Allow Field relaxation / addition in native job when appending to existing partitioned table
            val tableExists = Try(
              bigquery()
                .getTable(tableId)
                .exists()
            ).toOption.getOrElse(false)

            if (cliConfig.writeDisposition == WriteDisposition.WRITE_APPEND.toString && tableExists)
              queryConfig
                .setTimePartitioning(partitioning)
                .setSchemaUpdateOptions(
                  List(
                    SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                    SchemaUpdateOption.ALLOW_FIELD_RELAXATION
                  ).asJava
                )
            else
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
        updateTableDescription(tableId, cliConfig.outputTableDesc.getOrElse(""))
        updateColumnsDescription(getFieldsDescriptionSource(sql))
        BigQueryJobResult(Some(results), totalBytesProcessed)
      }
    }
  }

  def runBatchQuery(): Try[Job] = {
    getOrCreateDataset(None).flatMap { _ =>
      Try {
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
  }

  @deprecated("Views are now created using the syntax WTH ... AS ...", "0.1.25")
  def createViews(views: Map[String, String], udf: scala.Option[String]): Unit = {
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
