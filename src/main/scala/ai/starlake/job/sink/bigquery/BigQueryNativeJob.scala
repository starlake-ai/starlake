package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.job.ingest.{AuditLog, Step}
import ai.starlake.schema.model.{BigQuerySink, Format}
import ai.starlake.utils.{JobBase, JobResult, Utils}
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, SchemaUpdateOption, WriteDisposition}
import com.google.cloud.bigquery.JobStatistics.{LoadStatistics, QueryStatistics}
import com.google.cloud.bigquery.QueryJobConfiguration.Priority
import com.google.cloud.bigquery.{Schema => BQSchema, Table, _}
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.{BlobId, BlobInfo, Storage}
import org.apache.hadoop.fs.Path

import java.nio.file.Paths
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.Try

class BigQueryNativeJob(override val cliConfig: BigQueryLoadConfig, sql: String)(implicit
  val settings: Settings
) extends JobBase
    with BigQueryJobBase {
  override def name: String = s"bqload-${bqNativeTable}"

  logger.info(s"BigQuery Config $cliConfig")

  private def uploadFilesToGCS(sourceURIs: String): List[Path] = {

    val temporaryGcsBucket = connectionOptions
      .get("temporaryGcsBucket")
      .orElse(
        settings.comet.internal
          .flatMap(_.temporaryGcsBucket)
      )
      .getOrElse(
        throw new Exception(
          "Temporary GCS Bucket not defined. Please set env var SL_TEMPORARY_GCS_BUCKET"
        )
      )

    val folderId = UUID.randomUUID().toString
    val gcsPath =
      new Path(s"gs://${temporaryGcsBucket}/$folderId")
    sourceURIs
      .split(",")
      .map { sourceUri =>
        val sourcePath = new Path(sourceUri)
        val targetPathWithScheme = new Path(gcsPath, sourcePath.getName())
        val targetPath =
          Path.getPathWithoutSchemeAndAuthority(targetPathWithScheme)
        val targetObject = s"$folderId/${sourcePath.getName()}"
        val blobId = BlobId.of(temporaryGcsBucket, targetObject)
        val blobInfo = BlobInfo.newBuilder(blobId).build()
        val precondition: Storage.BlobWriteOption = Storage.BlobWriteOption.doesNotExist()
        val targetUri = Paths.get(sourcePath.toUri)

        this.gcsStorage().createFrom(blobInfo, targetUri, precondition)
        logger.info(s"Uploaded $sourcePath to $targetPathWithScheme")
        targetPathWithScheme
      }
      .toList
  }
  def loadPathsToBQ(bqSchema: BQSchema): Try[BigQueryJobResult] = {
    getOrCreateDataset(cliConfig.domainDescription).flatMap { _ =>
      Try {
        logger.info(s"BigQuery Schema: $bqSchema")
        val formatOptions: FormatOptions = bqLoadFormatOptions()
        cliConfig.source match {
          case Left(sourceURIs) =>
            val uri = sourceURIs.split(",").head

            // We upload local files first.
            val localFiles = uri.startsWith("file:")
            val gcsUris =
              if (localFiles)
                uploadFilesToGCS(sourceURIs).mkString(",")
              else
                sourceURIs

            val loadConfig: LoadJobConfiguration =
              bqLoadConfig(bqSchema, formatOptions, gcsUris)
            // Load data from a GCS CSV file into the table
            val job = bigquery().create(JobInfo.of(loadConfig))
            // Blocks until this load table job completes its execution, either failing or succeeding.
            val jobResult = job.waitFor()

            // We delete any updated local file
            if (localFiles) {
              gcsUris.split(",").foreach { uri =>
                val path = new Path(uri)
                settings.storageHandler().delete(path)
                logger.info(s"Deleted $path")
              }
            }
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
                BigQueryJobBase.getBqDatasetForNative(tableId),
                tableId.getTable,
                success = success,
                stats.getOutputRows + stats.getBadRecords,
                stats.getOutputRows,
                stats.getBadRecords,
                Timestamp.from(Instant.ofEpochMilli(stats.getStartTime)),
                stats.getEndTime - stats.getStartTime,
                if (success) "success" else s"${stats.getBadRecords} invalid records",
                Step.LOAD.toString,
                cliConfig.outputDatabase,
                settings.comet.tenant
              )
              settings.comet.audit.sink.getSink() match {
                case sink: BigQuerySink =>
                  AuditLog.sinkToBigQuery(log, sink)
                case _ =>
                  throw new Exception("Not Supported")
              }
              BigQueryJobResult(None, stats.getInputBytes, Some(jobResult))
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
  ): LoadJobConfiguration = {
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
    loadConfig.build()
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

  def runInteractiveQuery(thisSql: scala.Option[String] = None): Try[BigQueryJobResult] = {
    getOrCreateDataset(cliConfig.domainDescription).flatMap { _ =>
      Try {
        val targetSQL = thisSql.getOrElse(sql)
        val queryConfig: QueryJobConfiguration.Builder =
          QueryJobConfiguration
            .newBuilder(targetSQL)
            .setAllowLargeResults(true)
            .setJobTimeoutMs(
              connectionOptions.get("job-timeout-ms").map(java.lang.Long.valueOf).orNull
            )
            .setMaximumBytesBilled(
              connectionOptions.get("maximum-bytes-billed").map(java.lang.Long.valueOf).orNull
            )

        logger.info(s"Running interactive BQ Query $targetSQL")
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

        BigQueryJobResult(Some(results), totalBytesProcessed, Some(queryJob))
      }
    }
  }

  private def addUDFToQueryConfig(
    queryConfig: QueryJobConfiguration.Builder
  ): QueryJobConfiguration.Builder = {
    settings.comet
      .getUdfs()
      .foreach { udf =>
        queryConfig.setUserDefinedFunctions(List(UserDefinedFunction.fromUri(udf)).asJava)
      }
    queryConfig
  }

  /** Just to force any spark job to implement its entry point within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    if (cliConfig.materializedView) {
      RunAndSinkAsMaterializedView().map(table => BigQueryJobResult(None, 0L, None))
    } else {
      RunAndSinkAsTable()
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

  def RunAndSinkAsTable(thisSql: scala.Option[String] = None): Try[BigQueryJobResult] = {
    getOrCreateDataset(None).flatMap { targetDataset =>
      Try {
        val queryConfig: QueryJobConfiguration.Builder =
          QueryJobConfiguration
            .newBuilder(thisSql.getOrElse(sql))
            .setCreateDisposition(CreateDisposition.valueOf(cliConfig.createDisposition))
            .setWriteDisposition(WriteDisposition.valueOf(cliConfig.writeDisposition))
            .setDefaultDataset(targetDataset.getDatasetId)
            .setPriority(Priority.INTERACTIVE)
            .setJobTimeoutMs(
              connectionOptions.get("job-timeout-ms").map(java.lang.Long.valueOf).orNull
            )
            .setMaximumBytesBilled(
              connectionOptions.get("maximum-bytes-billed").map(java.lang.Long.valueOf).orNull
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
        val jobInfo = bigquery().create(JobInfo.of(finalConfiguration))
        val totalBytesProcessed = jobInfo
          .getStatistics()
          .asInstanceOf[QueryStatistics]
          .getTotalBytesProcessed

        val results = jobInfo.getQueryResults()
        logger.info(
          s"Query large results performed successfully: ${results.getTotalRows} rows inserted."
        )

        val table = bigquery().getTable(tableId)
        setTagsOnTable(table)

        applyRLSAndCLS().recover { case e =>
          Utils.logException(logger, e)
          throw new Exception(e)
        }
        updateTableDescription(table, cliConfig.outputTableDesc.getOrElse(""))
        updateColumnsDescription(getFieldsDescriptionSource(sql))
        BigQueryJobResult(Some(results), totalBytesProcessed, Some(jobInfo))
      }
    }
  }

  def runBatchQuery(wait: Boolean): Try[Job] = {
    getOrCreateDataset(None).flatMap { _ =>
      Try {
        val jobId = JobId
          .newBuilder()
          .setJob(
            UUID.randomUUID.toString
          ) // Run at batch priority, which won't count toward concurrent rate limit.
          .setLocation(connectionOptions.getOrElse("location", "EU"))
          .build()
        val queryConfig =
          QueryJobConfiguration
            .newBuilder(sql)
            .setPriority(Priority.BATCH)
            .setUseLegacySql(false)
            .setJobTimeoutMs(
              connectionOptions.get("job-timeout-ms").map(java.lang.Long.valueOf).orNull
            )
            .setMaximumBytesBilled(
              connectionOptions.get("maximum-bytes-billed").map(java.lang.Long.valueOf).orNull
            )
            .build()
        logger.info(s"Executing BQ Query $sql")
        val job =
          bigquery().create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build)
        logger.info(
          s"Batch query wth jobId $jobId sent to BigQuery "
        )
        if (job == null)
          throw new Exception("Job not executed since it no longer exists.")
        else {
          if (wait) {
            job.waitFor()
          }
          job
        }
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
