package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.extract.ParUtils
import ai.starlake.job.ingest.{BqLoadInfo, IngestionJob}
import ai.starlake.job.sink.bigquery.{
  BigQueryJobBase,
  BigQueryJobResult,
  BigQueryLoadConfig,
  BigQueryNativeJob
}
import ai.starlake.job.transform.BigQueryAutoTask
import ai.starlake.schema.model.*
import ai.starlake.utils.conversion.BigQueryUtils
import ai.starlake.utils.{IngestionCounters, JobResult, Utils}
import com.google.api.services.bigquery.model.TableSchema
import com.google.cloud.bigquery
import com.google.cloud.bigquery.{Field, JobInfo, StandardSQLTypeName, TableId}
import com.google.cloud.spark.bigquery.repackaged.com.google.api.client.json.gson.GsonFactory
import com.typesafe.scalalogging.StrictLogging

import java.time.Duration
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Random, Success, Try}

class BigQueryNativeLoader(ingestionJob: IngestionJob, accessToken: Option[String])(implicit
  settings: Settings
) extends NativeLoader(ingestionJob, accessToken)
    with StrictLogging {

  lazy val targetTableId: TableId =
    BigQueryJobBase.extractProjectDatasetAndTable(
      schemaHandler.getDatabase(domain),
      domain.finalName,
      effectiveSchema.finalName
    )

  def run(): Try[List[IngestionCounters]] = {
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
          ParUtils
            .runInParallel(path.map(_.toString).zipWithIndex, Some(settings.appConfig.maxParTask)) {
              case (sourceUri, index) =>
                val firstStepTempTable =
                  BigQueryJobBase.extractProjectDatasetAndTable(
                    schemaHandler.getDatabase(domain),
                    domain.finalName,
                    tempTableName + "_" + index
                  )
                val firstStepConfig =
                  targetConfig
                    .copy(
                      source = Left(sourceUri),
                      outputTableId = Some(firstStepTempTable),
                      outputTableDesc = Some("Temporary table created during data ingestion."),
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

                val enrichedTableInfo = firstStepTableInfo.copy(
                  maybeSchema = firstStepTableInfo.maybeSchema.map((schema: bigquery.Schema) =>
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
                  ),
                  maybeDurationMs = Some(Duration.ofHours(24).toMillis)
                )

                // TODO What if type is changed by transform ? we need to use safe_cast to have the same behavior as in SPARK.
                val firstStepResult =
                  firstStepBigqueryJob
                    .loadPathsToBQ(firstStepTableInfo, Some(enrichedTableInfo))
                (
                  firstStepResult,
                  firstStepTempTable,
                  enrichedTableInfo
                )
            }
            .foldLeft[(List[Try[BqLoadInfo]], List[TableId], List[TableInfo])]((Nil, Nil, Nil)) {
              case (
                    (loadResultList, tempTableIdList, tableInfoList),
                    (firstStepResult, firstStepTempTable, enrichedTableInfo)
                  ) =>
                (
                  loadResultList :+ firstStepResult,
                  tempTableIdList :+ firstStepTempTable,
                  tableInfoList :+ enrichedTableInfo
                )
            }
        def tryListSequence[A](list: List[Try[A]]): Try[List[A]] = {
          list.foldRight(Try(List.empty[A])) { (tryElem, acc) =>
            for {
              elem <- tryElem
              rest <- acc
            } yield elem :: rest
          }
        }

        val output: Try[List[BqLoadInfo]] =
          applyBigQuerySecondStep(
            targetConfig,
            List(
              BigQueryJobBase.extractProjectDatasetAndTable(
                schemaHandler.getDatabase(domain),
                domain.finalName,
                tempTableName + "_*"
              )
            ),
            tryListSequence(loadResults)
          )

        tempTableIds.zip(tableInfos).foreach { case (id, info) =>
          val database = Option(id.getProject()).getOrElse("")
          val schema = Option(id.getDataset()).getOrElse("")
          val table = Option(id.getTable()).getOrElse("")
          archiveTableTask(database, schema, table, info).foreach(_.run())
        }
        Try(ParUtils.runInParallel(tempTableIds, Some(settings.appConfig.maxParTask)) { tableId =>
          BigQueryJobBase.recoverBigqueryException {
            new BigQueryNativeJob(targetConfig, "").dropTable(tableId)
          }
        })
          .flatMap(_ => output)
          .recoverWith { case exception =>
            Utils.logException(logger, exception)
            output
          } // ignore exception but log it
      } else {
        val bigqueryJob = new BigQueryNativeJob(targetConfig, "")
        bigqueryJob
          .loadPathsToBQ(
            bigqueryJob.getTableInfo(targetTableId, _.targetBqSchemaWithoutIgnore(schemaHandler))
          )
          .map(List(_))
      }
    }.map {
      case Success(results) =>
        results.foreach {
          _.jobResult.job.flatMap(j => Option(j.getStatus.getExecutionErrors)).foreach { errors =>
            errors.forEach(err => logger.error(f"${err.getReason} - ${err.getMessage}"))
          }
        }
        val bqLoadInfoOutput = if (settings.appConfig.audit.detailedLoadAudit) {
          results
        } else {
          def combineStats(bqLoadInfo1: BqLoadInfo, bqLoadInfo2: BqLoadInfo): BqLoadInfo = {
            BqLoadInfo(
              totalAcceptedRows = bqLoadInfo1.totalAcceptedRows + bqLoadInfo2.totalAcceptedRows,
              totalRejectedRows = bqLoadInfo1.totalRejectedRows + bqLoadInfo2.totalRejectedRows,
              paths = bqLoadInfo1.paths ++ bqLoadInfo2.paths,
              jobResult = BigQueryJobResult(
                None,
                bqLoadInfo1.jobResult.totalBytesProcessed + bqLoadInfo2.jobResult.totalBytesProcessed,
                None
              )
            )
          }
          List(results.reduce(combineStats))
        }
        bqLoadInfoOutput.map { bli =>
          IngestionCounters(
            inputCount = bli.totalRows,
            acceptedCount = bli.totalAcceptedRows,
            rejectedCount = bli.totalRejectedRows,
            paths = bli.paths,
            jobid = ingestionJob.applicationId()
          )
        }
      case Failure(exception) =>
        Utils.logException(logger, exception)
        throw exception
    }
  }

  private def applyBigQuerySecondStep(
    targetConfig: BigQueryLoadConfig,
    firstStepTempTable: List[TableId],
    firstStepResult: Try[List[BqLoadInfo]]
  ): Try[List[BqLoadInfo]] = {
    firstStepResult match {
      case Success(loadFileResult) =>
        logger.info(s"First step result: ${loadFileResult.map(_.toString).mkString(", ")}")
        val targetBigqueryJob = new BigQueryNativeJob(targetConfig, "")
        val secondStepResult =
          targetBigqueryJob.cliConfig.outputTableId
            .map { _ =>
              applyBigQuerySecondStepSQL(
                firstStepTempTable.map(BigQueryUtils.tableIdToTableName)
              )
            }
            .getOrElse(throw new Exception("Should never happen"))

        secondStepResult
          .flatMap { _ =>
            firstStepResult
          }
      case res @ Failure(_) =>
        res
    }
  }

  private def applyBigQuerySecondStepSQL(
    firstStepTempTableTableNames: List[String]
  ): Try[JobResult] = {
    val task = this.secondStepSQLTask(firstStepTempTableTableNames)
    val bqTask = task.asInstanceOf[BigQueryAutoTask]
    val incomingSparkSchema = starlakeSchema.targetSparkSchemaWithoutIgnore(schemaHandler)
    bqTask.updateBigQueryTableSchema(incomingSparkSchema)
    val jobResult = bqTask.run()
    jobResult
  }
  override def buildSQLStatements(): Map[String, Object] = {
    val twoSteps = this.twoSteps
    val targetTableName = s"${domain.finalName}.${starlakeSchema.finalName}"
    val tempTableName = s"${domain.finalName}.${this.tempTableName}"
    val incomingDir = domain.resolveDirectory()
    val pattern = starlakeSchema.pattern.toString
    val format = mergedMetadata.resolveFormat()

    val incomingSparkSchema = starlakeSchema.targetSparkSchemaWithIgnoreAndScript(schemaHandler)
    val bqIncomingSchema = BigQueryUtils.bqSchema(incomingSparkSchema)

    val schemaString =
      Utils.newJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(bqIncomingSchema)

    val ddlMap: Map[String, Map[String, String]] = schemaHandler.getDdlMapping(starlakeSchema)

    val connectionPreActions =
      sinkConnection.options.get("preActions").map(_.split(';')).getOrElse(Array.empty).toList

    val stepMap =
      if (twoSteps) {

        val extraFileNameColumn =
          s"ALTER TABLE $tempTableName ADD COLUMN ${CometColumns.cometInputFileNameColumn} STRING DEFAULT '{{sl_input_file_name}}';"
        val workflowStatements = this.secondStepSQL(List(tempTableName))

        val dropFirstStepTableSql = s"DROP TABLE IF EXISTS $tempTableName;"
        val loadTaskSQL = Map(
          "steps"               -> "2",
          "incomingDir"         -> incomingDir,
          "pattern"             -> pattern,
          "format"              -> format,
          "firstStep"           -> "see schemastring",
          "extraFileNameColumn" -> List(extraFileNameColumn).asJava,
          "secondStep"          -> workflowStatements.task.asMap().asJava,
          "dropFirstStep"       -> dropFirstStepTableSql,
          "tempTableName"       -> tempTableName,
          "targetTableName"     -> targetTableName,
          "domain"              -> domain.finalName,
          "table"               -> starlakeSchema.finalName,
          "writeStrategy"       -> writeDisposition,
          "schemaString"        -> schemaString,
          "preActions"          -> connectionPreActions.asJava
        )

        workflowStatements
          .asMap()
          .updated(
            "statements",
            loadTaskSQL.asJava
          )
      } else {
        val workflowStatements = this.secondStepSQL(List(targetTableName))
        val loadTaskSQL =
          Map(
            "steps"           -> "1",
            "incomingDir"     -> incomingDir,
            "pattern"         -> pattern,
            "format"          -> format.toString,
            "createTable"     -> "see schemastring",
            "targetTableName" -> targetTableName,
            "domain"          -> domain.finalName,
            "table"           -> starlakeSchema.finalName,
            "writeStrategy"   -> writeDisposition,
            "schemaString"    -> schemaString,
            "preActions"      -> connectionPreActions.asJava
          )
        workflowStatements
          .asMap()
          .updated(
            "statements",
            loadTaskSQL.asJava
          )
      }
    val engine = settings.appConfig.jdbcEngines(engineName.toString)

    val tempStage = s"starlake_load_stage_${Random.alphanumeric.take(10).mkString("")}"
    val commonOptionsMap = Map(
      "schema"     -> starlakeSchema.asMap().asJava,
      "sink"       -> sink.asMap(engine).asJava,
      "fileSystem" -> settings.appConfig.fileSystem,
      "tempStage"  -> tempStage,
      "connection" -> sinkConnection.asMap(),
      "variant" -> starlakeSchema.attributes
        .exists(
          _.primitiveType(schemaHandler).getOrElse(PrimitiveType.string) == PrimitiveType.variant
        )
        .toString
    )
    val result = stepMap ++ commonOptionsMap
    result
  }

}
