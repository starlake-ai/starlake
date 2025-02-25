package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.exceptions.NullValueFoundException
import ai.starlake.job.ingest.{BqLoadInfo, IngestionJob}
import ai.starlake.job.sink.bigquery.{
  BigQueryJobBase,
  BigQueryJobResult,
  BigQueryLoadConfig,
  BigQueryNativeJob
}
import ai.starlake.job.transform.BigQueryAutoTask
import ai.starlake.schema.model._
import ai.starlake.utils.conversion.BigQueryUtils
import ai.starlake.utils.{IngestionCounters, JobResult, Utils}
import com.google.cloud.bigquery
import com.google.cloud.bigquery.{Field, JobInfo, StandardSQLTypeName, TableId}
import com.typesafe.scalalogging.StrictLogging

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class BigQueryNativeLoader(ingestionJob: IngestionJob, accessToken: Option[String])(implicit
  settings: Settings
) extends NativeLoader(ingestionJob, accessToken)
    with StrictLogging {
  lazy val effectiveSchema: Schema = computeEffectiveInputSchema()
  lazy val schemaWithMergedMetadata = effectiveSchema.copy(metadata = Some(mergedMetadata))

  lazy val targetTableId =
    BigQueryJobBase.extractProjectDatasetAndTable(
      schemaHandler.getDatabase(domain),
      domain.finalName,
      effectiveSchema.finalName
    )

  def run(): Try[IngestionCounters] = {
    Try {
      val bqSink = mergedMetadata.getSink().asInstanceOf[BigQuerySink]

      val targetConfig =
        BigQueryLoadConfig(
          connectionRef = Some(mergedMetadata.getSinkConnectionRef()),
          source = Left(path.map(_.toString).mkString(",")),
          outputTableId = Some(targetTableId),
          sourceFormat = settings.appConfig.defaultWriteFormat,
          createDisposition = createDisposition,
          writeDisposition = writeDisposition,
          outputPartition = bqSink.getPartitionColumn(),
          outputClustering = bqSink.clustering.getOrElse(Nil),
          days = bqSink.days,
          requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
          rls = effectiveSchema.rls,
          partitionsToUpdate = Nil,
          starlakeSchema = Some(schemaWithMergedMetadata),
          domainTags = domain.tags,
          domainDescription = domain.comment,
          outputDatabase = schemaHandler.getDatabase(domain),
          accessToken = accessToken
        )
      if (twoSteps) {
        val (loadResults, tempTableIds, tableInfos) =
          path
            .map(_.toString)
            .foldLeft[(List[Try[BqLoadInfo]], List[TableId], List[TableInfo])]((Nil, Nil, Nil)) {
              case ((loadResultList, tempTableIdList, tableInfoList), sourceUri) =>
                val firstStepTempTable =
                  BigQueryJobBase.extractProjectDatasetAndTable(
                    schemaHandler.getDatabase(domain),
                    domain.finalName,
                    tempTableName
                  )
                val firstStepConfig =
                  targetConfig
                    .copy(
                      source = Left(sourceUri),
                      outputTableId = Some(firstStepTempTable),
                      outputTableDesc = Some("Temporary table created during data ingestion."),
                      days = Some(1),
                      // force first step to be write append, otherwise write_truncate overwrite the
                      // created structure with default values, making second step query to fail if it relies on
                      // technical column such as comet_input_filename.
                      writeDisposition = JobInfo.WriteDisposition.WRITE_APPEND.name()
                    )

                val firstStepBigqueryJob = new BigQueryNativeJob(firstStepConfig, "")
                val firstStepTableInfo = firstStepBigqueryJob.getTableInfo(
                  firstStepTempTable,
                  _.targetBqSchemaWithIgnoreAndScript(schemaHandler)
                )

                val enrichedTableInfo = firstStepTableInfo.copy(maybeSchema =
                  firstStepTableInfo.maybeSchema.map((schema: bigquery.Schema) =>
                    bigquery.Schema.of(
                      (schema.getFields.asScala :+
                      Field
                        .newBuilder(
                          CometColumns.cometInputFileNameColumn,
                          StandardSQLTypeName.STRING
                        )
                        .setDefaultValueExpression(s"'$sourceUri'")
                        .build()).asJava
                    )
                  )
                )

                // TODO What if type is changed by transform ? we need to use safe_cast to have the same behavior as in SPARK.
                val firstStepResult =
                  firstStepBigqueryJob.loadPathsToBQ(firstStepTableInfo, Some(enrichedTableInfo))
                (
                  loadResultList :+ firstStepResult,
                  tempTableIdList :+ firstStepTempTable,
                  tableInfoList :+ enrichedTableInfo
                )
            }
        def combineStats(bqLoadInfo1: BqLoadInfo, bqLoadInfo2: BqLoadInfo): BqLoadInfo = {
          BqLoadInfo(
            bqLoadInfo1.totalAcceptedRows + bqLoadInfo2.totalAcceptedRows,
            bqLoadInfo1.totalRejectedRows + bqLoadInfo2.totalRejectedRows,
            jobResult = BigQueryJobResult(
              None,
              bqLoadInfo1.jobResult.totalBytesProcessed + bqLoadInfo2.jobResult.totalBytesProcessed,
              None
            )
          )
        }
        val globalLoadResult: Try[BqLoadInfo] = loadResults.reduce { (result1, result2) =>
          result1.flatMap(r => result2.map(combineStats(_, r)))
        }

        val output: Try[BqLoadInfo] =
          applyBigQuerySecondStep(
            targetConfig,
            tempTableIds,
            globalLoadResult
          )

        tempTableIds.zip(tableInfos).foreach { case (id, info) =>
          val database = Option(id.getProject()).getOrElse("")
          val schema = Option(id.getDataset()).getOrElse("")
          val table = Option(id.getTable()).getOrElse("")
          archiveTableTask(database, schema, table, info).foreach(_.run())
        }
        Try(tempTableIds.foreach(new BigQueryNativeJob(targetConfig, "").dropTable))
          .flatMap(_ => output)
          .recoverWith { case exception =>
            Utils.logException(logger, exception)
            output
          } // ignore exception but log it
      } else {
        val bigqueryJob = new BigQueryNativeJob(targetConfig, "")
        bigqueryJob.loadPathsToBQ(
          bigqueryJob.getTableInfo(targetTableId, _.targetBqSchemaWithoutIgnore(schemaHandler))
        )
      }
    }.map {
      case res @ Success(result) =>
        result.jobResult.job.flatMap(j => Option(j.getStatus.getExecutionErrors)).foreach {
          errors =>
            errors.forEach(err => logger.error(f"${err.getReason} - ${err.getMessage}"))
        }
        IngestionCounters(
          result.totalRows,
          result.totalAcceptedRows,
          result.totalRejectedRows
        )
      case res @ Failure(exception) =>
        Utils.logException(logger, exception)
        throw exception
    }
  }

  private def applyBigQuerySecondStep(
    targetConfig: BigQueryLoadConfig,
    firstStepTempTable: List[TableId],
    firstStepResult: Try[BqLoadInfo]
  ): Try[BqLoadInfo] = {
    firstStepResult match {
      case Success(loadFileResult) =>
        logger.info(s"First step result: $loadFileResult")
        val targetBigqueryJob = new BigQueryNativeJob(targetConfig, "")
        val secondStepResult =
          targetBigqueryJob.cliConfig.outputTableId
            .map { _ =>
              applyBigQuerySecondStepSQL(
                firstStepTempTable.map(BigQueryUtils.tableIdToTableName)
              )
            }
            .getOrElse(throw new Exception("Should never happen"))

        def updateRejectedCount(nullCountValues: Long): Try[BqLoadInfo] = {
          firstStepResult.map(r =>
            r.copy(
              totalAcceptedRows = r.totalAcceptedRows - nullCountValues,
              totalRejectedRows = r.totalRejectedRows + nullCountValues
            )
          )
        }

        secondStepResult
          .flatMap { _ =>
            updateRejectedCount(0)
          } // keep loading stats
          .recoverWith { case ex: NullValueFoundException =>
            updateRejectedCount(ex.nbRecord)
          }
      case res @ Failure(_) =>
        res
    }
  }

  private def getArchiveTableComponents(): (Option[String], String, String) = {
    val fullArchiveTableName = Utils.parseJinja(
      settings.appConfig.archiveTablePattern,
      Map("domain" -> domain.finalName, "table" -> starlakeSchema.finalName)
    )
    val archiveTableComponents = fullArchiveTableName.split('.')
    val (archiveDatabaseName, archiveDomainName, archiveTableName) =
      if (archiveTableComponents.length == 3) {
        (
          Some(archiveTableComponents(0)),
          archiveTableComponents(1),
          archiveTableComponents(2)
        )
      } else if (archiveTableComponents.length == 2) {
        (
          schemaHandler.getDatabase(domain),
          archiveTableComponents(0),
          archiveTableComponents(1)
        )
      } else {
        throw new Exception(
          s"Archive table name must be in the format <domain>.<table> but got $fullArchiveTableName"
        )
      }
    (archiveDatabaseName, archiveDomainName, archiveTableName)
  }

  def applyBigQuerySecondStepSQL(
    firstStepTempTableTableNames: List[String]
  ): Try[JobResult] = {
    val task = this.secondStepSQLTask(firstStepTempTableTableNames)
    val bqTask = task.asInstanceOf[BigQueryAutoTask]
    val incomingSparkSchema = starlakeSchema.targetSparkSchemaWithoutIgnore(schemaHandler)
    bqTask.updateBigQueryTableSchema(incomingSparkSchema)
    val jobResult = bqTask.run()
    jobResult
  }

}
