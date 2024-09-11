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
import ai.starlake.job.transform.{AutoTask, BigQueryAutoTask}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.conversion.BigQueryUtils
import ai.starlake.utils.{IngestionCounters, JobResult, Utils}
import com.google.cloud.bigquery
import com.google.cloud.bigquery.{Field, StandardSQLTypeName, TableId}
import com.typesafe.scalalogging.StrictLogging
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.hadoop.fs.Path

import java.nio.charset.Charset
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try, Using}

class BigQueryNativeLoader(ingestionJob: IngestionJob, accessToken: Option[String])(implicit
  val settings: Settings
) extends StrictLogging {

  val domain: Domain = ingestionJob.domain

  val schema: Schema = ingestionJob.schema

  val storageHandler: StorageHandler = ingestionJob.storageHandler

  val schemaHandler: SchemaHandler = ingestionJob.schemaHandler

  val path: List[Path] = ingestionJob.path

  val options: Map[String, String] = ingestionJob.options

  val strategy: WriteStrategy = ingestionJob.mergedMetadata.getStrategyOptions()

  lazy val mergedMetadata: Metadata = ingestionJob.mergedMetadata

  private def requireTwoSteps(schema: Schema): Boolean = {
    // renamed attribute can be loaded directly so it's not in the condition
    schema
      .hasTransformOrIgnoreOrScriptColumns() ||
    strategy.isMerge() ||
    schema.filter.nonEmpty ||
    settings.appConfig.archiveTable
  }

  def run(): Try[IngestionCounters] = {
    Try {
      val effectiveSchema: Schema = computeEffectiveInputSchema()
      val (createDisposition: String, writeDisposition: String) = Utils.getDBDisposition(
        strategy.toWriteMode()
      )
      val bqSink = mergedMetadata.getSink().asInstanceOf[BigQuerySink]
      val schemaWithMergedMetadata = effectiveSchema.copy(metadata = Some(mergedMetadata))

      val targetTableId =
        BigQueryJobBase.extractProjectDatasetAndTable(
          schemaHandler.getDatabase(domain),
          domain.finalName,
          effectiveSchema.finalName
        )

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
      val twoSteps = requireTwoSteps(effectiveSchema)
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
                    SQLUtils.temporaryTableName(effectiveSchema.finalName)
                  )
                val firstStepConfig =
                  targetConfig
                    .copy(
                      source = Left(sourceUri),
                      outputTableId = Some(firstStepTempTable),
                      outputTableDesc = Some("Temporary table created during data ingestion."),
                      days = Some(1)
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
        tempTableIds.zip(tableInfos).foreach { case (id, info) => archiveTable(id, info) }
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

  private def archiveTable(firstStepTempTable: TableId, firstStepTableInfo: TableInfo) = {
    if (settings.appConfig.archiveTable) {
      val (
        archiveDatabaseName: Option[String],
        archiveDomainName: String,
        archiveTableName: String
      ) = getArchiveTableComponents()

      val targetTable = OutputRef(
        firstStepTempTable.getProject(),
        firstStepTempTable.getDataset(),
        firstStepTempTable.getTable()
      ).toSQLString(mergedMetadata.getSink().getConnection())
      val firstStepFields = firstStepTableInfo.maybeSchema
        .map { schema =>
          schema.getFields.asScala.map(_.getName)
        }
        .getOrElse(
          throw new Exception(
            "Should never happen in Ingestion mode. We know the fields we are loading using the yml files"
          )
        )
      val req =
        s"SELECT ${firstStepFields.mkString(",")}, '${ingestionJob.applicationId()}' as JOBID FROM $targetTable"
      val taskDesc = AutoTaskDesc(
        s"archive-${ingestionJob.applicationId()}",
        Some(req),
        database = archiveDatabaseName,
        archiveDomainName,
        archiveTableName,
        sink = Some(mergedMetadata.getSink().toAllSinks())
      )

      val autoTask = AutoTask.task(
        Option(ingestionJob.applicationId()),
        taskDesc,
        Map.empty,
        None,
        truncate = false,
        test = false,
        Engine.BQ,
        logExecution = true
      )(
        settings,
        storageHandler,
        schemaHandler
      )
      autoTask.run()
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
                firstStepTempTable,
                schema
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
      /*
          .flatMap { case (_, nullCountValues) =>
            updateRejectedCount(nullCountValues)
          } // keep loading stats
          .recoverWith { case ex: NullValueFoundException =>
            updateRejectedCount(ex.nbRecord)
          }

       */
      case res @ Failure(_) =>
        res
    }
  }

  private def getArchiveTableComponents(): (Option[String], String, String) = {
    val fullArchiveTableName = Utils.parseJinja(
      settings.appConfig.archiveTablePattern,
      Map("domain" -> domain.finalName, "table" -> schema.finalName)
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

  private def computeEffectiveInputSchema(): Schema = {
    mergedMetadata.resolveFormat() match {
      case Format.DSV =>
        (mergedMetadata.resolveWithHeader(), path.map(_.toString).headOption) match {
          case (java.lang.Boolean.TRUE, Some(sourceFile)) =>
            val csvHeaders = storageHandler.readAndExecute(
              new Path(sourceFile),
              Charset.forName(mergedMetadata.resolveEncoding())
            ) { is =>
              Using.resource(is) { reader =>
                assert(
                  mergedMetadata.resolveQuote().length <= 1,
                  "quote must be a single character"
                )
                assert(
                  mergedMetadata.resolveEscape().length <= 1,
                  "quote must be a single character"
                )
                val csvParserSettings = new CsvParserSettings()
                val format = new CsvFormat()
                format.setDelimiter(mergedMetadata.resolveSeparator())
                mergedMetadata.resolveQuote().headOption.foreach(format.setQuote)
                mergedMetadata.resolveEscape().headOption.foreach(format.setQuoteEscape)
                csvParserSettings.setFormat(format)
                // allocate twice the declared columns. If fail a strange exception is thrown: https://github.com/uniVocity/univocity-parsers/issues/247
                csvParserSettings.setMaxColumns(schema.attributes.length * 2)
                csvParserSettings.setNullValue(mergedMetadata.resolveNullValue())
                csvParserSettings.setHeaderExtractionEnabled(true)
                csvParserSettings.setMaxCharsPerColumn(-1)
                val csvParser = new CsvParser(csvParserSettings)
                csvParser.beginParsing(reader)
                // call this in order to get the headers even if there is no record
                csvParser.parseNextRecord()
                csvParser.getRecordMetadata.headers().toList
              }
            }
            val attributesMap = schema.attributes.map(attr => attr.name -> attr).toMap
            val csvAttributesInOrders =
              csvHeaders.map(h =>
                attributesMap.getOrElse(h, Attribute(h, ignore = Some(true), required = None))
              )
            // attributes not in csv input file must not be required but we don't force them to optional.
            val effectiveAttributes =
              csvAttributesInOrders ++ schema.attributes.diff(csvAttributesInOrders)
            if (effectiveAttributes.length > schema.attributes.length) {
              logger.warn(
                s"Attributes in the CSV file are bigger from the schema. " +
                s"Schema will be updated to match the CSV file. " +
                s"Schema: ${schema.attributes.map(_.name).mkString(",")}. " +
                s"CSV: ${csvHeaders.mkString(",")}"
              )
              schema.copy(attributes = effectiveAttributes.take(schema.attributes.length))

            } else {
              schema.copy(attributes = effectiveAttributes)
            }

          case _ => schema
        }
      case _ => schema
    }
  }

  def applyBigQuerySecondStepSQL(
    firstStepTempTableId: List[TableId],
    starlakeSchema: Schema
  ): Try[JobResult] = {
    val incomingSparkSchema = starlakeSchema.targetSparkSchemaWithoutIgnore(schemaHandler)

    val tempTable = firstStepTempTableId
      .map(BigQueryUtils.tableIdToTableName)
      .map("SELECT * FROM " + _)
      .mkString("(", " UNION ALL ", ")")
    val targetTableName = s"${domain.finalName}.${schema.finalName}"

    val sqlWithTransformedFields =
      starlakeSchema.buildSqlSelectOnLoad(tempTable)

    val taskDesc = AutoTaskDesc(
      name = targetTableName,
      sql = Some(sqlWithTransformedFields),
      database = schemaHandler.getDatabase(domain),
      domain = domain.finalName,
      table = schema.finalName,
      presql = schema.presql,
      postsql = schema.postsql,
      sink = mergedMetadata.sink,
      rls = schema.rls,
      expectations = schema.expectations,
      acl = schema.acl,
      comment = schema.comment,
      tags = schema.tags,
      writeStrategy = mergedMetadata.writeStrategy,
      parseSQL = Some(true)
    )
    val job =
      new BigQueryAutoTask(
        Option(ingestionJob.applicationId()),
        taskDesc,
        Map.empty,
        None,
        truncate = false,
        test = false,
        logExecution = true
      )(
        settings,
        storageHandler,
        schemaHandler
      )

    job.updateBigQueryTableSchema(incomingSparkSchema)
    val jobResult = job.run()
    jobResult
  }

}
