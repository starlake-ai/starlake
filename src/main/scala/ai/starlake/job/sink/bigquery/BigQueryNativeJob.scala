package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.job.ingest.NativeBqLoadInfo
import ai.starlake.schema.model.{
  ClusteringInfo,
  FieldPartitionInfo,
  Format,
  Schema,
  TableInfo => SLTableInfo
}
import ai.starlake.utils.{JobBase, JobResult, Utils}
import better.files.File
import com.google.cloud.RetryOption
import com.google.cloud.bigquery.BigQuery.QueryResultsOption
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, SchemaUpdateOption, WriteDisposition}
import com.google.cloud.bigquery.JobStatistics.{LoadStatistics, QueryStatistics}
import com.google.cloud.bigquery.QueryJobConfiguration.Priority
import com.google.cloud.bigquery.{Schema => BQSchema, Table, _}

import java.net.URI
import java.nio.channels.Channels
import java.nio.file.Files
import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try, Using}

class BigQueryNativeJob(
  override val cliConfig: BigQueryLoadConfig,
  sql: String,
  resultPageSize: Long = 1,
  jobTimeoutMs: scala.Option[Long] = None
)(implicit
  val settings: Settings
) extends JobBase
    with BigQueryJobBase {
  override def name: String = s"bqload-${bqNativeTable}"

  logger.info(s"BigQuery Config $cliConfig")

  def loadPathsToBQ(tableInfo: SLTableInfo): Try[NativeBqLoadInfo] = {
    getOrCreateTable(cliConfig.domainDescription, tableInfo, None).flatMap { _ =>
      Try {
        val bqSchema =
          tableInfo.maybeSchema.getOrElse(throw new RuntimeException("Should never happen"))
        logger.info(s"BigQuery Schema: $bqSchema")
        val formatOptions: FormatOptions = bqLoadFormatOptions()
        cliConfig.source match {
          case Left(sourceURIs) =>
            val uri = sourceURIs.split(",").head

            // We upload local files first.
            val localFiles = uri.startsWith("file:")
            recoverBigqueryException {
              val job = {
                if (localFiles) {
                  loadLocalFilePathsToBQ(bqSchema, formatOptions, sourceURIs.split(","))
                } else {
                  val loadConfig: LoadJobConfiguration =
                    bqLoadConfig(bqSchema, formatOptions, sourceURIs)
                  // Load data from a GCS CSV file into the table
                  val jobId = newJobIdWithLocation()
                  bigquery().create(JobInfo.newBuilder(loadConfig).setJobId(jobId).build())
                }
              }
              // Blocks until this load table job completes its execution, either failing or succeeding.
              job.waitFor(
                RetryOption.totalTimeout(
                  org.threeten.bp.Duration.ofMillis(
                    jobTimeoutMs.getOrElse(settings.appConfig.longJobTimeoutMs)
                  )
                )
              )
            }.map { jobResult =>
              if (scala.Option(jobResult.getStatus.getError()).isEmpty) {
                val stats = jobResult.getStatistics.asInstanceOf[LoadStatistics]
                applyRLSAndCLS().recover { case e =>
                  Utils.logException(logger, e)
                  throw e
                }
                logger.info(
                  s"bq-ingestion-summary -> files: [$sourceURIs], domain: ${tableId.getDataset}, schema: ${tableId.getTable}, input: ${stats.getOutputRows + stats.getBadRecords}, accepted: ${stats.getOutputRows}, rejected:${stats.getBadRecords}"
                )
                NativeBqLoadInfo(
                  stats.getOutputRows,
                  stats.getBadRecords,
                  BigQueryJobResult(None, stats.getInputBytes, Some(jobResult))
                )
              } else
                throw new Exception(
                  "BigQuery was unable to load into the table due to an error:" + jobResult.getStatus
                    .getError()
                )
            }
          case Right(_) =>
            throw new Exception("Should never happen")
        }
      }.flatten
    }
  }

  private def loadLocalFilePathsToBQ(
    bqSchema: BQSchema,
    formatOptions: FormatOptions,
    sourceURIs: Iterable[String]
  ): Job = {
    val loadConfig = bqLoadLocaFileConfig(bqSchema, formatOptions)
    val jobId: JobId = newJobIdWithLocation()
    Using(bigquery().writer(jobId, loadConfig)) { writer =>
      val outputStream = Channels.newOutputStream(writer)
      sourceURIs
        .foreach(uri => Files.copy(File(new URI(uri)).path, outputStream))
    }
    bigquery().getJob(jobId)
  }

  private def newJobIdWithLocation(): JobId = {
    val jobName = UUID.randomUUID().toString();
    val jobIdBuilder = JobId.newBuilder().setJob(jobName);
    cliConfig.outputDatabase.map(jobIdBuilder.setProject)
    jobIdBuilder.setLocation(
      connectionOptions.getOrElse(
        "location",
        throw new Exception(
          s"location is required but not present in connection $connectionName"
        )
      )
    )
    val jobId = jobIdBuilder.build()
    jobId
  }

  def getTableInfo(tableId: TableId, toBQSchema: Schema => BQSchema) = {
    SLTableInfo(
      tableId,
      cliConfig.outputTableDesc,
      cliConfig.starlakeSchema.map(toBQSchema),
      cliConfig.outputPartition.map { partitionField =>
        FieldPartitionInfo(
          partitionField,
          cliConfig.days,
          cliConfig.requirePartitionFilter
        )
      },
      cliConfig.outputClustering match {
        case Nil => None
        case _   => Some(ClusteringInfo(cliConfig.outputClustering.toList))
      }
    )
  }

  private def bqLoadConfig(
    bqSchema: BQSchema,
    formatOptions: FormatOptions,
    sourceURIs: String
  ): LoadJobConfiguration = {
    val loadConfig =
      LoadJobConfiguration.newBuilder(tableId, sourceURIs.split(",").toList.asJava, formatOptions)
    configureBqLoad(loadConfig, bqSchema).build()
  }

  private def bqLoadLocaFileConfig(
    bqSchema: BQSchema,
    formatOptions: FormatOptions
  ): WriteChannelConfiguration = {
    val loadConfig = WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(formatOptions)
    configureBqLoad(loadConfig, bqSchema).build()
  }

  private def configureBqLoad[T <: LoadConfiguration.Builder](
    loadConfig: T,
    bqSchema: BQSchema
  ): T = {
    loadConfig
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
    }
    if (!settings.appConfig.rejectAllOnError)
      loadConfig.setMaxBadRecords(settings.appConfig.rejectMaxRecords)

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
    cliConfig.starlakeSchema.flatMap(_.metadata) match {
      case Some(metadata) =>
        metadata.getFormat() match {
          case Format.DSV =>
            metadata.nullValue.foreach(loadConfig.setNullMarker)
          case _ =>
        }
      case _ =>
    }
    loadConfig
  }

  private def bqLoadFormatOptions(): FormatOptions = {
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
            formatOptions.setAllowJaggedRows(true)
            formatOptions.build()
          case Format.JSON | Format.SIMPLE_JSON =>
            FormatOptions.json()
          case _ =>
            throw new Exception(s"Should never happen: ${metadata.getFormat()}")
        }
      case None =>
        throw new Exception("Should never happen")
    }
    formatOptions
  }

  def runInteractiveQuery(
    thisSql: scala.Option[String] = None,
    pageSize: scala.Option[Long] = None,
    queryJobTimeoutMs: scala.Option[Long] = None
  ): Try[BigQueryJobResult] = {
    getOrCreateDataset(cliConfig.domainDescription).flatMap { _ =>
      Try {
        val targetSQL = thisSql.getOrElse(sql).trim()
        val queryConfig: QueryJobConfiguration.Builder =
          QueryJobConfiguration
            .newBuilder(targetSQL)
            .setAllowLargeResults(true)
            .setMaximumBytesBilled(
              connectionOptions.get("maximumBytesBilled").map(java.lang.Long.valueOf).orNull
            )

        logger.info(s"Running interactive BQ Query $targetSQL")
        val queryConfigWithUDF = addUDFToQueryConfig(queryConfig)
        val finalConfiguration = queryConfigWithUDF.setPriority(Priority.INTERACTIVE).build()

        recoverBigqueryException {
          val jobId = newJobIdWithLocation()
          val queryJob =
            bigquery().create(JobInfo.newBuilder(finalConfiguration).setJobId(jobId).build())
          queryJob.waitFor(
            RetryOption.maxAttempts(0),
            RetryOption.totalTimeout(
              org.threeten.bp.Duration.ofMinutes(
                queryJobTimeoutMs
                  .orElse(jobTimeoutMs)
                  .getOrElse(settings.appConfig.longJobTimeoutMs)
              )
            )
          )
        }.map { jobResult =>
          val totalBytesProcessed = jobResult
            .getStatistics()
            .asInstanceOf[QueryStatistics]
            .getTotalBytesProcessed

          val results = jobResult.getQueryResults(
            QueryResultsOption.pageSize(pageSize.getOrElse(this.resultPageSize))
          )
          logger.info(
            s"Query large results performed successfully: ${results.getTotalRows} rows returned."
          )

          BigQueryJobResult(Some(results), totalBytesProcessed, Some(jobResult))
        }
      }.flatten
    }
  }

  private def addUDFToQueryConfig(
    queryConfig: QueryJobConfiguration.Builder
  ): QueryJobConfiguration.Builder = {
    settings.appConfig
      .getUdfs()
      .foreach { udf =>
        if (udf.contains("://")) // make sure it's a URI
          queryConfig.setUserDefinedFunctions(List(UserDefinedFunction.fromUri(udf)).asJava)
      }
    queryConfig
  }

  /** Just to force any job to implement its entry point within the "run" method
    *
    * @return
    *   : job result
    */
  override def run(): Try[JobResult] = {
    // The very first time we update a audit table, we run it interactively to make sure there is not
    // risk running it in batch mode (batch mode cannot catch errors such as schema incompatibility).
    if (this.datasetId.getDataset() == settings.appConfig.audit.getDomain()) {
      if (settings.appConfig.internal.map(_.bqAuditSaveInBatchMode).getOrElse(true)) {
        runBatchQuery().map(_ => BigQueryJobResult(None, 0L, None))
      } else {
        RunAndSinkAsTable(queryJobTimeoutMs = jobTimeoutMs)
      }
    } else if (cliConfig.materializedView) {
      RunAndSinkAsMaterializedView().map(table => BigQueryJobResult(None, 0L, None))
    } else {
      RunAndSinkAsTable(queryJobTimeoutMs = jobTimeoutMs)
    }
  }

  def RunAndSinkAsMaterializedView(thisSql: scala.Option[String] = None): Try[Table] = {
    getOrCreateDataset(None).flatMap { _ =>
      Try {
        val materializedViewDefinitionBuilder =
          MaterializedViewDefinition.newBuilder(thisSql.getOrElse(sql))
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

        cliConfig.enableRefresh match {
          case Some(x) => materializedViewDefinitionBuilder.setEnableRefresh(x)
          case None    =>
        }
        cliConfig.refreshIntervalMs match {
          case Some(x) => materializedViewDefinitionBuilder.setRefreshIntervalMs(x.toLong)
          case None    =>
        }
        val table =
          bigquery().create(TableInfo.of(tableId, materializedViewDefinitionBuilder.build()))
        setTagsOnTable(table)
        table
      }
    }
  }

  def RunAndSinkAsTable(
    thisSql: scala.Option[String] = None,
    targetTableSchema: scala.Option[BQSchema] = None,
    queryJobTimeoutMs: scala.Option[Long] = None
  ): Try[BigQueryJobResult] = {
    getOrCreateDataset(None).flatMap { targetDataset =>
      Try {
        val queryConfig: QueryJobConfiguration.Builder =
          QueryJobConfiguration
            .newBuilder(thisSql.getOrElse(sql))
            .setCreateDisposition(CreateDisposition.valueOf(cliConfig.createDisposition))
            .setWriteDisposition(WriteDisposition.valueOf(cliConfig.writeDisposition))
            .setDefaultDataset(targetDataset.getDatasetId)
            .setPriority(Priority.INTERACTIVE)
            .setMaximumBytesBilled(
              connectionOptions.get("maximumBytesBilled").map(java.lang.Long.valueOf).orNull
            )
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
        recoverBigqueryException {
          val jobId = newJobIdWithLocation()
          val jobInfo =
            bigquery().create(JobInfo.newBuilder(finalConfiguration).setJobId(jobId).build())

          jobInfo.waitFor(
            RetryOption.totalTimeout(
              org.threeten.bp.Duration.ofMinutes(
                queryJobTimeoutMs
                  .orElse(jobTimeoutMs)
                  .getOrElse(settings.appConfig.longJobTimeoutMs)
              )
            )
          )
        }.map { jobResult =>
          val totalBytesProcessed = jobResult
            .getStatistics()
            .asInstanceOf[QueryStatistics]
            .getTotalBytesProcessed
          val results = jobResult.getQueryResults(QueryResultsOption.pageSize(this.resultPageSize))
          logger.info(
            s"Query large results performed successfully: ${results.getTotalRows} rows inserted and processed ${totalBytesProcessed} bytes."
          )
          val table = bigquery().getTable(tableId)
          setTagsOnTable(table)

          applyRLSAndCLS().recover { case e =>
            Utils.logException(logger, e)
            throw new Exception(e)
          }
          updateTableDescription(table, cliConfig.outputTableDesc.getOrElse(""))
          targetTableSchema
            .map(updateColumnsDescription)
            .getOrElse(
              updateColumnsDescription(
                BigQueryJobBase.dictToBQSchema(getFieldsDescriptionSource(sql))
              )
            )
          BigQueryJobResult(Some(results), totalBytesProcessed, Some(jobResult))
        }
      }.flatten
    }
  }

  // run batch query use only (for auditing only)
  def runBatchQuery(
    thisSql: scala.Option[String] = None,
    wait: Boolean = false
  ): Try[Job] = {
    getOrCreateDataset(None).flatMap { targetDataset =>
      Try {
        val jobId = newJobIdWithLocation()
        val queryConfig =
          QueryJobConfiguration
            .newBuilder(thisSql.getOrElse(sql))
            .setCreateDisposition(CreateDisposition.valueOf(cliConfig.createDisposition))
            .setWriteDisposition(WriteDisposition.valueOf(cliConfig.writeDisposition))
            .setDefaultDataset(targetDataset.getDatasetId)
            .setDestinationTable(tableId)
            .setPriority(Priority.BATCH)
            .setUseLegacySql(false)
            .setSchemaUpdateOptions(
              List(
                SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                SchemaUpdateOption.ALLOW_FIELD_RELAXATION
              ).asJava
            )
            .setMaximumBytesBilled(
              connectionOptions.get("maximumBytesBilled").map(java.lang.Long.valueOf).orNull
            )
            .build()
        logger.info(s"Executing batch BQ Query $sql")
        val job =
          bigquery().create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build)
        logger.info(
          s"Batch query wth jobId $jobId sent to BigQuery "
        )
        if (job == null)
          throw new Exception(s"Job for $sql not executed since it no longer exists.")
        else {
          if (wait) {
            job.waitFor(
              RetryOption.totalTimeout(
                org.threeten.bp.Duration
                  .ofMinutes(jobTimeoutMs.getOrElse(settings.appConfig.longJobTimeoutMs))
              )
            )
          } else {
            logger.info(s"${job.getJobId()} is running in background")
          }
          job
        }
      } match {
        case Failure(e) =>
          logger.error(s"Error while running batch query $sql", e)
          Failure(e)
        case Success(job) =>
          Success(job)
      }
    }
  }
}
