package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.exceptions.NullValueFoundException
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig, BigQueryNativeJob}
import ai.starlake.job.transform.{AutoTask, BigQueryAutoTask}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.conversion.BigQueryUtils
import ai.starlake.utils.{JobResult, Utils}
import com.google.cloud.bigquery.{
  Field,
  LegacySQLTypeName,
  Schema => BQSchema,
  StandardTableDefinition,
  TableId
}
import com.typesafe.scalalogging.StrictLogging
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.hadoop.fs.Path

import java.nio.charset.Charset
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try, Using}

class BigQueryNativeIngestionJob(ingestionJob: IngestionJob)(implicit val settings: Settings)
    extends StrictLogging {
  val domain: Domain = ingestionJob.domain

  val schema: Schema = ingestionJob.schema

  val storageHandler: StorageHandler = ingestionJob.storageHandler

  val schemaHandler: SchemaHandler = ingestionJob.schemaHandler

  val path: List[Path] = ingestionJob.path

  val options: Map[String, String] = ingestionJob.options

  val strategy: StrategyOptions = schema.getStrategy(Some(ingestionJob.mergedMetadata))

  lazy val mergedMetadata: Metadata = ingestionJob.mergedMetadata

  private def requireTwoSteps(schema: Schema, sink: BigQuerySink): Boolean = {
    // renamed attribute can be loaded directly so it's not in the condition
    schema
      .hasTransformOrIgnoreOrScriptColumns() ||
    schema.strategy.nonEmpty ||
    schema.filter.nonEmpty ||
    sink.dynamicPartitionOverwrite.getOrElse(false) ||
    settings.appConfig.archiveTable
  }

  def run(): Try[IngestionCounters] = {
    Try {
      val effectiveSchema: Schema = computeEffectiveInputSchema()
      val (createDisposition: String, writeDisposition: String) = Utils.getDBDisposition(
        strategy.`type`.toWriteMode()
      )
      val bqSink = mergedMetadata.getSink().asInstanceOf[BigQuerySink]
      val schemaWithMergedMetadata = effectiveSchema.copy(metadata = Some(mergedMetadata))

      val targetTableId =
        BigQueryJobBase.extractProjectDatasetAndTable(
          schemaHandler.getDatabase(domain),
          domain.finalName,
          effectiveSchema.finalName
        )

      val targetConfig = ingestionJob
        .buildCommonNativeBQLoadConfig(
          createDisposition,
          writeDisposition,
          bqSink,
          schemaWithMergedMetadata
        )
        .copy(
          outputTableId = Some(targetTableId),
          days = bqSink.days,
          outputPartition = bqSink.timestamp,
          outputClustering = bqSink.clustering.getOrElse(Nil),
          requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
          rls = effectiveSchema.rls
        )
      val twoSteps = requireTwoSteps(effectiveSchema, bqSink)
      if (twoSteps) {
        val firstStepTempTable =
          BigQueryJobBase.extractProjectDatasetAndTable(
            schemaHandler.getDatabase(domain),
            domain.finalName,
            SQLUtils.temporaryTableName(effectiveSchema.finalName)
          )
        val firstStepConfig = ingestionJob
          .buildCommonNativeBQLoadConfig(
            createDisposition,
            writeDisposition,
            bqSink,
            schemaWithMergedMetadata
          )
          .copy(
            outputTableId = Some(firstStepTempTable),
            outputTableDesc = Some("Temporary table created during data ingestion."),
            days = Some(1)
          )
        val firstStepBigqueryJob = new BigQueryNativeJob(firstStepConfig, "")
        val firstStepTableInfo = firstStepBigqueryJob.getTableInfo(
          firstStepTempTable,
          _.bqSchemaWithIgnoreAndScript(schemaHandler)
        )

        // TODO What if type is changed by transform ? we need to use safe_cast to have the same behavior as in SPARK.
        val firstStepResult =
          firstStepBigqueryJob.loadPathsToBQ(firstStepTableInfo)

        val targetTableSchema: BQSchema = effectiveSchema.bqSchemaWithoutIgnore(schemaHandler)
        val output: Try[BqLoadInfo] =
          applyBigQuerySecondStep(
            targetTableSchema,
            targetConfig,
            firstStepTempTable,
            firstStepResult
          )
        archiveTable(firstStepTempTable, firstStepTableInfo)
        Try(firstStepBigqueryJob.dropTable(firstStepTempTable))
          .flatMap(_ => output)
          .recoverWith { case exception =>
            Utils.logException(logger, exception)
            output
          } // ignore exception but log it
      } else {
        val bigqueryJob = new BigQueryNativeJob(targetConfig, "")
        bigqueryJob.loadPathsToBQ(
          bigqueryJob.getTableInfo(targetTableId, _.bqSchemaWithoutIgnore(schemaHandler))
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
        Some(WriteMode.APPEND),
        sink = Some(mergedMetadata.getSink().toAllSinks())
      )

      val autoTask = AutoTask.task(
        taskDesc,
        Map.empty,
        None,
        truncate = false,
        Engine.BQ
      )(
        settings,
        storageHandler,
        schemaHandler
      )
      autoTask.run()
    }
  }

  private def applyBigQuerySecondStep(
    targetTableSchema: BQSchema,
    targetConfig: BigQueryLoadConfig,
    firstStepTempTable: TableId,
    firstStepResult: Try[BqLoadInfo]
  ): Try[BqLoadInfo] = {
    firstStepResult match {
      case Success(loadFileResult) =>
        logger.info(s"First step result: $loadFileResult")
        val targetBigqueryJob = new BigQueryNativeJob(targetConfig, "")
        val secondStepResult =
          targetBigqueryJob.cliConfig.outputTableId
            .map { _ =>
              BigQueryNativeIngestionJob.updateTargetTableSchema(
                targetBigqueryJob,
                targetTableSchema,
                strategy.`type` == StrategyType.SCD2,
                schema.comment
              )
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
    mergedMetadata.getFormat() match {
      case Format.DSV =>
        (mergedMetadata.isWithHeader(), path.map(_.toString).headOption) match {
          case (java.lang.Boolean.TRUE, Some(sourceFile)) =>
            val csvHeaders = storageHandler.readAndExecute(
              new Path(sourceFile),
              Charset.forName(mergedMetadata.getEncoding())
            ) { is =>
              Using.resource(is) { reader =>
                assert(mergedMetadata.getQuote().length <= 1, "quote must be a single character")
                assert(mergedMetadata.getEscape().length <= 1, "quote must be a single character")
                val csvParserSettings = new CsvParserSettings()
                val format = new CsvFormat()
                format.setDelimiter(mergedMetadata.getSeparator())
                mergedMetadata.getQuote().headOption.foreach(format.setQuote)
                mergedMetadata.getEscape().headOption.foreach(format.setQuoteEscape)
                csvParserSettings.setFormat(format)
                // allocate twice the declared columns. If fail a strange exception is thrown: https://github.com/uniVocity/univocity-parsers/issues/247
                csvParserSettings.setMaxColumns(schema.attributes.length * 2)
                csvParserSettings.setNullValue(mergedMetadata.getNullValue())
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
                attributesMap.getOrElse(h, Attribute(h, ignore = Some(true), required = false))
              )
            // attributes not in csv input file must not be required but we don't force them to optional.
            val effectiveAttributes =
              csvAttributesInOrders ++ schema.attributes.diff(csvAttributesInOrders)
            schema.copy(attributes = effectiveAttributes)
          case _ => schema
        }
      case _ => schema
    }
  }

  def applyBigQuerySecondStepSQL(
    firstStepTempTableId: TableId,
    starlakeSchema: Schema
  ): Try[JobResult] = {
    val tempTable = BigQueryUtils.tableIdToTableName(firstStepTempTableId)
    val sourceUris = path.map(_.toString).mkString(",").replace("'", "\\'")
    val targetTableName = s"${domain.finalName}.${schema.finalName}"

    val sqlWithTransformedFields = starlakeSchema.buildSqlSelectOnLoad(
      tempTable,
      Some(sourceUris),
      "`",
      applyTransformAndIgnore = true
    )

    val taskDesc = AutoTaskDesc(
      name = targetTableName,
      sql = Some(sqlWithTransformedFields),
      database = schemaHandler.getDatabase(domain),
      domain = domain.finalName,
      table = schema.finalName,
      write = Some(mergedMetadata.getWrite()),
      presql = schema.presql,
      postsql = schema.postsql,
      sink = mergedMetadata.sink,
      rls = schema.rls,
      expectations = schema.expectations,
      acl = schema.acl,
      comment = schema.comment,
      tags = schema.tags,
      strategy = schema.strategy,
      parseSQL = Some(true)
    )
    val jobResult =
      new BigQueryAutoTask(taskDesc, Map.empty, None, truncate = false)(
        settings,
        storageHandler,
        schemaHandler
      ).run()
    jobResult
  }

}

object BigQueryNativeIngestionJob extends StrictLogging {

  /** return all partitions and the number of null records */
  private def computePartitions(
    bigqueryJob: BigQueryNativeJob,
    partitionName: String,
    sql: String
  ): (List[String], Long) = {
    val totalColumnName = "total"
    val detectImpliedPartitions =
      s"SELECT cast(date(`$partitionName`) as STRING) AS $partitionName, countif($partitionName IS NULL) AS $totalColumnName FROM ($sql) GROUP BY $partitionName"
    bigqueryJob.runInteractiveQuery(Some(detectImpliedPartitions), pageSize = Some(1000)) match {
      case Failure(exception) => throw exception
      case Success(result) =>
        val (partitions, nullCountValues) = result.tableResult
          .map(
            _.iterateAll()
              .iterator()
              .asScala
              .foldLeft(List[String]() -> 0L) { case ((partitions, nullCount), row) =>
                val updatedPartitions = scala
                  .Option(row.get(partitionName))
                  .filterNot(_.isNull)
                  .map(_.getStringValue) match {
                  case Some(value) => value +: partitions
                  case None        => partitions
                }
                updatedPartitions -> (nullCount + row.get(totalColumnName).getLongValue)
              }
          )
          .getOrElse(Nil -> 0L)
        partitions.sorted -> nullCountValues
    }
  }

  def updateTargetTableSchema(
    bigqueryJob: BigQueryNativeJob,
    incomingTableSchema: BQSchema,
    isSCD2: Boolean,
    tableComment: Option[String]
  )(implicit settings: Settings): Try[StandardTableDefinition] = {
    val incomingTableSchemaWithSCD2 =
      if (
        isSCD2 && !incomingTableSchema.getFields.asScala.exists(
          _.getName == settings.appConfig.scd2StartTimestamp
        )
      ) {
        val startCol = Field
          .newBuilder(
            settings.appConfig.scd2StartTimestamp,
            LegacySQLTypeName.TIMESTAMP
          )
          .setMode(Field.Mode.NULLABLE)
          .build()
        val endCol = Field
          .newBuilder(
            settings.appConfig.scd2EndTimestamp,
            LegacySQLTypeName.TIMESTAMP
          )
          .setMode(Field.Mode.NULLABLE)
          .build()
        val allFields = incomingTableSchema.getFields.asScala.toList :+ startCol :+ endCol
        BQSchema.of(allFields.asJava)
      } else
        incomingTableSchema

    val tableId = bigqueryJob.tableId
    if (bigqueryJob.tableExists(tableId)) {
      val existingTableSchema = bigqueryJob.getBQSchema(tableId)
      // detect new columns
      val newColumns = incomingTableSchemaWithSCD2.getFields.asScala
        .filterNot(field =>
          existingTableSchema.getFields.asScala.exists(_.getName == field.getName)
        )
        .toList
      // Update target table schema with new columns if any
      if (newColumns.nonEmpty) {
        bigqueryJob.updateTableSchema(tableId, incomingTableSchema)
      } else
        Try(bigqueryJob.getTableDefinition(tableId))
    } else {
      val config = bigqueryJob.cliConfig
      val partitionField = config.outputPartition.map { partitionField =>
        FieldPartitionInfo(partitionField, config.days, config.requirePartitionFilter)
      }
      val clusteringFields = config.outputClustering match {
        case Nil    => None
        case fields => Some(ClusteringInfo(fields.toList))
      }
      bigqueryJob.getOrCreateTable(
        config.domainDescription,
        TableInfo(
          tableId,
          tableComment,
          Some(incomingTableSchemaWithSCD2),
          partitionField,
          clusteringFields
        ),
        None
      ) map { case (table, definition) => definition }
    }
  }

}
