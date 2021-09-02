package com.ebiznext.comet.job.ingest

import com.ebiznext.comet.config.{DatasetArea, Settings, StorageArea}
import com.ebiznext.comet.job.index.bqload.{BigQueryLoadConfig, BigQuerySparkJob}
import com.ebiznext.comet.job.index.connectionload.{ConnectionLoadConfig, ConnectionLoadJob}
import com.ebiznext.comet.job.index.esload.{ESLoadConfig, ESLoadJob}
import com.ebiznext.comet.job.ingest.ImprovedDataFrameContext._
import com.ebiznext.comet.job.metrics.{AssertionJob, MetricsJob}
import com.ebiznext.comet.job.validator.{GenericRowValidator, ValidationResult}
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.Rejection.{ColInfo, ColResult}
import com.ebiznext.comet.schema.model.Trim.{BOTH, LEFT, RIGHT}
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.Formatter._
import com.ebiznext.comet.utils.conversion.BigQueryUtils
import com.ebiznext.comet.utils.kafka.KafkaClient
import com.ebiznext.comet.utils.{JobResult, SparkJob, SparkJobResult, Utils}
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.{
  Field,
  LegacySQLTypeName,
  Schema => BQSchema,
  StandardTableDefinition
}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  ArrayType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}
import scala.collection.JavaConverters._
import scala.language.existentials
import scala.util.{Failure, Success, Try}

trait IngestionJob extends SparkJob {

  protected val treeRowValidator: GenericRowValidator = Utils
    .loadInstance[GenericRowValidator](settings.comet.treeValidatorClass)

  protected val flatRowValidator: GenericRowValidator = Utils
    .loadInstance[GenericRowValidator](settings.comet.rowValidatorClass)

  def domain: Domain

  def schema: Schema

  def storageHandler: StorageHandler

  def schemaHandler: SchemaHandler

  def types: List[Type]

  def path: List[Path]

  def options: Map[String, String]

  val now: Timestamp = java.sql.Timestamp.from(Instant.now)

  /** Merged metadata
    */
  lazy val metadata: Metadata = schema.mergedMetadata(domain.metadata)

  protected def loadDataSet(): Try[DataFrame]

  /** ingestion algorithm
    *
    * @param dataset
    */
  protected def ingest(dataset: DataFrame): (RDD[_], RDD[_])

  protected def applyIgnore(dfIn: DataFrame): Dataset[Row] = {
    import session.implicits._
    metadata.ignore.map { ignore =>
      if (ignore.startsWith("udf:")) {
        dfIn.filter(
          !callUDF(ignore.substring("udf:".length), struct(dfIn.columns.map(dfIn(_)): _*))
        )
      } else {
        dfIn.filter(!($"value" rlike ignore))
      }
    } getOrElse dfIn
  }

  protected def saveRejected(
    errMessagesRDD: RDD[String],
    rejectedLinesRDD: RDD[String]
  ): Try[Path] = {
    logger.whenDebugEnabled {
      logger.debug(s"rejectedRDD SIZE ${errMessagesRDD.count()}")
      errMessagesRDD.take(100).foreach(rejected => logger.debug(rejected.replaceAll("\n", "|")))
    }
    val domainName = domain.name
    val schemaName = schema.name

    val start = Timestamp.from(Instant.now())
    val formattedDate = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(start)

    if (settings.comet.sinkReplayToFile && !rejectedLinesRDD.isEmpty()) {
      val replayArea = DatasetArea.replay(domainName)
      val targetPath =
        new Path(replayArea, s"$domainName.$schemaName.$formattedDate.replay")
      rejectedLinesRDD
        .coalesce(1)
        .saveAsTextFile(targetPath.toString)
      storageHandler.moveSparkPartFile(
        targetPath,
        "0000" // When saving as text file, no extension is added.
      )
    }

    IngestionUtil.sinkRejected(session, errMessagesRDD, domainName, schemaName, now) match {
      case Success((rejectedDF, rejectedPath)) =>
        // We sink to a file when running unit tests
        if (settings.comet.sinkToFile) {
          sinkToFile(
            rejectedDF,
            rejectedPath,
            WriteMode.APPEND,
            StorageArea.rejected,
            merge = false,
            settings.comet.defaultRejectedWriteFormat
          )
        } else {
          settings.comet.audit.sink match {
            case _: NoneSink | FsSink(_, _, _, _) =>
              sinkToFile(
                rejectedDF,
                rejectedPath,
                WriteMode.APPEND,
                StorageArea.rejected,
                merge = false,
                settings.comet.defaultRejectedWriteFormat
              )
            case _ => // do nothing
          }
        }
        val end = Timestamp.from(Instant.now())
        val log = AuditLog(
          session.sparkContext.applicationId,
          rejectedPath.toString,
          domainName,
          schemaName,
          success = true,
          -1,
          -1,
          -1,
          start,
          end.getTime - start.getTime,
          "success",
          Step.SINK_REJECTED.toString
        )
        AuditLog.sink(session, log)
        Success(rejectedPath)
      case Failure(exception) =>
        logger.error("Failed to save Rejected", exception)
        val end = Timestamp.from(Instant.now())
        val log = AuditLog(
          session.sparkContext.applicationId,
          new Path(DatasetArea.rejected(domainName), schemaName).toString,
          domainName,
          schemaName,
          success = false,
          -1,
          -1,
          -1,
          start,
          end.getTime - start.getTime,
          Utils.exceptionAsString(exception),
          Step.LOAD.toString
        )
        AuditLog.sink(session, log)
        Failure(exception)
    }
  }

  def getWriteMode(): WriteMode =
    schema.merge
      .map(_ => WriteMode.OVERWRITE)
      .getOrElse(metadata.getWrite())

  lazy val (format, extension) = metadata.sink
    .map {
      case sink: FsSink =>
        (sink.format.getOrElse(""), sink.extension.getOrElse(""))
      case _ =>
        ("", "")
    }
    .getOrElse(("", ""))

  private def csvOutput(): Boolean =
    (settings.comet.csvOutput || format == "csv") &&
    !settings.comet.grouped &&
    metadata.partition.isEmpty && path.nonEmpty

  private def csvOutputExtension(): String =
    if (settings.comet.csvOutputExt.nonEmpty)
      settings.comet.csvOutputExt
    else
      extension

  private def runAssertions(acceptedDF: DataFrame) = {
    if (settings.comet.assertions.active) {
      new AssertionJob(
        this.domain.name,
        this.schema.name,
        this.schema.assertions.getOrElse(Map.empty),
        Stage.UNIT,
        storageHandler,
        schemaHandler,
        Some(acceptedDF),
        Engine.SPARK,
        sql => session.sql(sql).count()
      ).run().getOrElse(throw new Exception("Should never happen"))
    }
  }

  private def runMetrics(acceptedDF: DataFrame) = {
    if (settings.comet.metrics.active) {
      new MetricsJob(this.domain, this.schema, Stage.UNIT, this.storageHandler, this.schemaHandler)
        .run(acceptedDF, System.currentTimeMillis())
    }
  }

  private def dfWithAttributesRenamed(acceptedDF: DataFrame): DataFrame = {
    val renamedAttributes = schema.renamedAttributes().toMap
    logger.whenInfoEnabled {
      renamedAttributes.foreach { case (name, rename) =>
        logger.info(s"renaming column $name to $rename")
      }
    }
    val finalDF =
      renamedAttributes.foldLeft(acceptedDF) { case (acc, (name, rename)) =>
        acc.withColumnRenamed(existingName = name, newName = rename)
      }
    finalDF
  }

  /** Merge new and existing dataset if required Save using overwrite / Append mode
    *
    * @param acceptedDF
    */
  protected def saveAccepted(
    dataframe: DataFrame,
    validationResult: ValidationResult
  ): (DataFrame, Path) = {
    if (!settings.comet.rejectAllOnError || validationResult.rejected.isEmpty()) {
      val acceptedDF = dfWithAttributesRenamed(dataframe)
      val start = Timestamp.from(Instant.now())
      logger.whenDebugEnabled {
        logger.debug(s"acceptedRDD SIZE ${acceptedDF.count()}")
        acceptedDF.show(1000)
      }
      runAssertions(acceptedDF)
      runMetrics(acceptedDF)
      val acceptedPath = new Path(DatasetArea.accepted(domain.name), schema.name)
      val acceptedDfWithScriptFields: DataFrame = computeScriptedAttributes(acceptedDF)
      val acceptedDfWithoutIgnoredFields: DataFrame = removeIgnoredAttributes(
        acceptedDfWithScriptFields
      )
      val finalAcceptedDF: DataFrame = computeFinalSchema(acceptedDfWithoutIgnoredFields)
      val (mergedDF, partitionsToUpdate) = applyMerge(acceptedPath, finalAcceptedDF)

      val finalMergedDf: DataFrame = runPostSQL(mergedDF)

      val writeMode = getWriteMode()

      logger.info("Final Dataframe Schema")
      finalMergedDf.printSchema()
      val savedInFileDataset =
        if (settings.comet.sinkToFile)
          sinkToFile(
            finalMergedDf,
            acceptedPath,
            writeMode,
            StorageArea.accepted,
            schema.merge.isDefined,
            settings.comet.defaultWriteFormat
          )
        else
          finalMergedDf

      val sinkType = metadata.getSink().map(_.`type`)
      val savedDataset = sinkType.getOrElse(SinkType.None) match {
        case SinkType.FS | SinkType.None if !settings.comet.sinkToFile =>
          // TODO do this inside the sink function below
          sinkToFile(
            finalMergedDf,
            acceptedPath,
            writeMode,
            StorageArea.accepted,
            schema.merge.isDefined,
            settings.comet.defaultWriteFormat
          )
        case _ =>
          savedInFileDataset
      }
      logger.info("Saved Dataset Schema")
      savedDataset.printSchema()
      sink(finalMergedDf, partitionsToUpdate) match {
        case Success(_) =>
          val end = Timestamp.from(Instant.now())
          val log = AuditLog(
            session.sparkContext.applicationId,
            acceptedPath.toString,
            domain.name,
            schema.name,
            success = true,
            -1,
            -1,
            -1,
            start,
            end.getTime - start.getTime,
            "success",
            Step.SINK_ACCEPTED.toString
          )
          AuditLog.sink(session, log)
        case Failure(exception) =>
          Utils.logException(logger, exception)
          val end = Timestamp.from(Instant.now())
          val log = AuditLog(
            session.sparkContext.applicationId,
            acceptedPath.toString,
            domain.name,
            schema.name,
            success = false,
            -1,
            -1,
            -1,
            start,
            end.getTime - start.getTime,
            Utils.exceptionAsString(exception),
            Step.SINK_ACCEPTED.toString
          )
          AuditLog.sink(session, log)
          throw exception
      }
      (savedDataset, acceptedPath)
    } else {
      (session.emptyDataFrame, new Path("invalid-path"))
    }
  }

  private def runPostSQL(mergedDF: DataFrame) = {
    val finalMergedDf = schema.postsql match {
      case Some(queryList) =>
        queryList.foldLeft(mergedDF) { (df, query) =>
          df.createOrReplaceTempView("COMET_TABLE")
          df.sparkSession.sql(query)
        }
      case _ => mergedDF
    }
    finalMergedDf
  }

  private def applyMerge(
    acceptedPath: Path,
    finalAcceptedDF: DataFrame
  ): (DataFrame, Option[List[String]]) = {
    val (mergedDF, partitionsToUpdate) = schema.merge
      .map { mergeOptions =>
        if (metadata.getSink().map(_.`type`).getOrElse(SinkType.None) == SinkType.BQ) {
          mergeFromBQ(finalAcceptedDF, mergeOptions)
        } else {
          (mergeFromParquet(acceptedPath, finalAcceptedDF, mergeOptions), None)
        }
      }
      .getOrElse((finalAcceptedDF, None))
    (mergedDF, partitionsToUpdate)
  }

  private def computeFinalSchema(acceptedDfWithoutIgnoredFields: DataFrame) = {
    val finalAcceptedDF: DataFrame = schema.attributes.exists(_.script.isDefined) match {
      case true => {
        logger.whenDebugEnabled {
          logger.debug("Accepted Dataframe schema right after adding computed columns")
          acceptedDfWithoutIgnoredFields.printSchema()
        }
        // adding computed columns can change the order of columns, we must force the order defined in the schema
        val orderedWithScriptFieldsDF =
          session.createDataFrame(
            acceptedDfWithoutIgnoredFields.rdd,
            schema.finalSparkSchema(schemaHandler)
          )
        logger.whenDebugEnabled {
          logger.debug("Accepted Dataframe schema after applying the defined schema")
          acceptedDfWithoutIgnoredFields.printSchema()
        }
        orderedWithScriptFieldsDF
      }
      case false => acceptedDfWithoutIgnoredFields
    }
    finalAcceptedDF
  }

  private def removeIgnoredAttributes(acceptedDfWithScriptFields: DataFrame) = {
    val ignoredAttributes = schema.attributes.filter(_.isIgnore()).map(_.getFinalName())
    val acceptedDfWithoutIgnoredFields = acceptedDfWithScriptFields.drop(ignoredAttributes: _*)
    acceptedDfWithoutIgnoredFields
  }

  private def computeScriptedAttributes(acceptedDF: DataFrame) = {
    val acceptedDfWithScriptFields = (if (schema.attributes.exists(_.script.isDefined)) {
                                        val allColumns = "*"
                                        schema.attributes.foldLeft(acceptedDF) {
                                          case (
                                                df,
                                                Attribute(
                                                  name,
                                                  _,
                                                  _,
                                                  _,
                                                  _,
                                                  _,
                                                  _,
                                                  _,
                                                  _,
                                                  _,
                                                  _,
                                                  _,
                                                  _,
                                                  Some(script),
                                                  _,
                                                  _
                                                )
                                              ) =>
                                            df.T(
                                              s"SELECT $allColumns, ${script.richFormat(options)} as $name FROM __THIS__"
                                            )
                                          case (df, _) => df
                                        }
                                      } else acceptedDF).drop(Settings.cometInputFileNameColumn)
    acceptedDfWithScriptFields
  }

  private def sink(
    mergedDF: DataFrame,
    partitionsToUpdate: Option[List[String]]
  ): Try[Unit] = {
    Try {
      val sinkType = metadata.getSink().map(_.`type`)
      sinkType.getOrElse(SinkType.None) match {
        case SinkType.ES if settings.comet.elasticsearch.active =>
          val sink = metadata.getSink().map(_.asInstanceOf[EsSink])
          val config = ESLoadConfig(
            timestamp = sink.flatMap(_.timestamp),
            id = sink.flatMap(_.id),
            format = "parquet",
            domain = domain.name,
            schema = schema.name,
            dataset = Some(Right(mergedDF)),
            options = sink.map(_.getOptions).getOrElse(Map.empty)
          )
          new ESLoadJob(config, storageHandler, schemaHandler).run()
        case SinkType.ES if !settings.comet.elasticsearch.active =>
          logger.warn("Indexing to ES requested but elasticsearch not active in conf file")
        case SinkType.BQ =>
          val sink = metadata.getSink().map(_.asInstanceOf[BigQuerySink])
          val (createDisposition: String, writeDisposition: String) = Utils.getDBDisposition(
            metadata.getWrite(),
            schema.merge.exists(_.key.nonEmpty)
          )

          /* We load the schema from the postsql returned dataframe if any */
          val tableSchema = schema.postsql match {
            case Some(_) => Some(BigQueryUtils.bqSchema(mergedDF.schema))
            case _       => Some(schema.bqSchema(schemaHandler))
          }
          val config = BigQueryLoadConfig(
            source = Right(mergedDF),
            outputTable = schema.name,
            outputDataset = domain.name,
            sourceFormat = "parquet",
            createDisposition = createDisposition,
            writeDisposition = writeDisposition,
            location = sink.flatMap(_.location),
            outputPartition = sink.flatMap(_.timestamp),
            outputClustering = sink.flatMap(_.clustering).getOrElse(Nil),
            days = sink.flatMap(_.days),
            requirePartitionFilter = sink.flatMap(_.requirePartitionFilter).getOrElse(false),
            rls = schema.rls,
            options = sink.map(_.getOptions).getOrElse(Map.empty),
            partitionsToUpdate = partitionsToUpdate
          )
          val res = new BigQuerySparkJob(config, tableSchema).run()
          res match {
            case Success(_) => ;
            case Failure(e) =>
              throw e
          }

        case SinkType.KAFKA =>
          Utils.withResources(new KafkaClient(settings.comet.kafka)) { kafkaClient =>
            kafkaClient.sinkToTopic(settings.comet.kafka.topics(schema.name), mergedDF)
          }
        case SinkType.JDBC =>
          val (createDisposition: CreateDisposition, writeDisposition: WriteDisposition) = {

            val (cd, wd) = Utils.getDBDisposition(
              metadata.getWrite(),
              schema.merge.exists(_.key.nonEmpty)
            )
            (CreateDisposition.valueOf(cd), WriteDisposition.valueOf(wd))
          }
          val sink = metadata.getSink().map(_.asInstanceOf[JdbcSink])
          sink.foreach { sink =>
            val partitions = sink.partitions.getOrElse(1)
            val batchSize = sink.batchsize.getOrElse(1000)
            val jdbcName = sink.connection

            val jdbcConfig = ConnectionLoadConfig.fromComet(
              jdbcName,
              settings.comet,
              Right(mergedDF),
              outputTable = schema.name,
              createDisposition = createDisposition,
              writeDisposition = writeDisposition,
              partitions = partitions,
              batchSize = batchSize,
              options = sink.getOptions
            )

            val res = new ConnectionLoadJob(jdbcConfig).run()
            res match {
              case Success(_) => ;
              case Failure(e) =>
                throw e
            }
          }
        case SinkType.None | SinkType.FS =>
          // Done in the caller
          // TODO do it here instead
          logger.trace("not producing an index, as requested (no sink or sink at None explicitly)")
      }
    }
  }

  /** Save typed dataset in parquet. If hive support is active, also register it as a Hive Table and
    * if analyze is active, also compute basic statistics
    *
    * @param dataset
    *   : dataset to save
    * @param targetPath
    *   : absolute path
    * @param writeMode
    *   : Append or overwrite
    * @param area
    *   : accepted or rejected area
    */
  private def sinkToFile(
    dataset: DataFrame,
    targetPath: Path,
    writeMode: WriteMode,
    area: StorageArea,
    merge: Boolean,
    writeFormat: String
  ): DataFrame = {
    val resultDataFrame = if (dataset.columns.length > 0) {
      val saveMode = writeMode.toSaveMode
      val hiveDB = StorageArea.area(domain.name, area)
      val tableName = schema.name
      val fullTableName = s"$hiveDB.$tableName"
      if (settings.comet.hive) {
        val dbComment = domain.comment.getOrElse("")
        session.sql(s"create database if not exists $hiveDB comment '$dbComment'")
        session.sql(s"use $hiveDB")
        Try {
          if (writeMode.toSaveMode == SaveMode.Overwrite)
            session.sql(s"drop table if exists $hiveDB.$tableName")
        } match {
          case Success(_) => ;
          case Failure(e) =>
            logger.warn("Ignore error when hdfs files not found")
            Utils.logException(logger, e)
        }
      }

      val tmpPath = new Path(s"${targetPath.toString}.tmp")

      val nbPartitions = metadata.getSamplingStrategy() match {
        case 0.0 => // default partitioning
          if (csvOutput() || dataset.rdd.getNumPartitions == 0) // avoid error for an empty dataset
            1
          else
            dataset.rdd.getNumPartitions
        case fraction if fraction > 0.0 && fraction < 1.0 =>
          // Use sample to determine partitioning
          val count = dataset.count()
          val minFraction =
            if (fraction * count >= 1) // Make sure we get at least on item in teh dataset
              fraction
            else if (
              count > 0
            ) // We make sure we get at least 1 item which is 2 because of double imprecision for huge numbers.
              2 / count
            else
              0

          val sampledDataset = dataset.sample(withReplacement = false, minFraction)
          partitionedDatasetWriter(sampledDataset, metadata.getPartitionAttributes())
            .mode(SaveMode.ErrorIfExists)
            .format(writeFormat)
            .option("path", tmpPath.toString)
            .save()
          val consumed = storageHandler.spaceConsumed(tmpPath) / fraction
          val blocksize = storageHandler.blockSize(tmpPath)
          storageHandler.delete(tmpPath)
          Math.max(consumed / blocksize, 1).toInt
        case count if count >= 1.0 =>
          count.toInt
      }

      // No need to apply partition on rejected dF
      val partitionedDFWriter =
        if (area == StorageArea.rejected)
          partitionedDatasetWriter(dataset.coalesce(nbPartitions), Nil)
        else
          partitionedDatasetWriter(
            dataset.coalesce(nbPartitions),
            metadata.getPartitionAttributes()
          )

      val mergePath = s"${targetPath.toString}.merge"
      val (targetDatasetWriter, finalDataset) = if (merge && area != StorageArea.rejected) {
        logger.info(s"Saving Dataset to merge location $mergePath")
        partitionedDFWriter
          .mode(SaveMode.Overwrite)
          .format(writeFormat)
          .option("path", mergePath)
          .save()
        logger.info(s"reading Dataset from merge location $mergePath")
        val mergedDataset = session.read.parquet(mergePath)
        (
          partitionedDatasetWriter(
            mergedDataset,
            metadata.getPartitionAttributes()
          ),
          mergedDataset
        )
      } else
        (partitionedDFWriter, dataset)
      val finalTargetDatasetWriter =
        if (csvOutput() && area != StorageArea.rejected) {
          targetDatasetWriter
            .mode(saveMode)
            .format("csv")
            .option("ignoreLeadingWhiteSpace", value = false)
            .option("ignoreTrailingWhiteSpace", value = false)
            .option("header", metadata.withHeader.getOrElse(false))
            .option("delimiter", metadata.separator.getOrElse("µ"))
            .option("path", targetPath.toString)
        } else
          targetDatasetWriter
            .mode(saveMode)
            .format(writeFormat)
            .option("path", targetPath.toString)

      logger.info(s"Saving Dataset to final location $targetPath in $saveMode mode")

      if (settings.comet.hive) {
        finalTargetDatasetWriter.saveAsTable(fullTableName)
        val tableComment = schema.comment.getOrElse("")
        session.sql(s"ALTER TABLE $fullTableName SET TBLPROPERTIES ('comment' = '$tableComment')")
        analyze(fullTableName)
      } else {
        finalTargetDatasetWriter.save()
      }
      if (merge && area != StorageArea.rejected) {
        // Here we read the df from the targetPath and not the merged one since that on is gonna be removed
        // However, we keep the merged DF schema so we don't lose any metadata from reloading the final parquet (especially the nullables)
        val df = session.createDataFrame(
          session.read.parquet(targetPath.toString).rdd,
          dataset.schema
        )
        storageHandler.delete(new Path(mergePath))
        logger.info(s"deleted merge file $mergePath")
        df
      } else
        finalDataset
    } else {
      logger.warn("Empty dataset with no columns won't be saved")
      session.emptyDataFrame
    }
    if (csvOutput() && area != StorageArea.rejected) {
      val outputList = storageHandler
        .list(targetPath, ".csv", LocalDateTime.MIN, recursive = false)
        .filterNot(path => schema.pattern.matcher(path.getName).matches())
      if (outputList.nonEmpty) {
        val csvPath = outputList.head
        val finalCsvPath =
          if (csvOutputExtension().nonEmpty) {
            // Explicitily set extension
            val targetName = path.head.getName
            val index = targetName.lastIndexOf('.')
            val finalName = (if (index > 0) targetName.substring(0, index)
                             else targetName) + csvOutputExtension()
            new Path(targetPath, finalName)
          } else
            new Path(
              targetPath,
              path.head.getName
            )
        storageHandler.move(csvPath, finalCsvPath)
      }
    }
    // output file should have the same name as input file when applying privacy
    if (
      settings.comet.defaultWriteFormat == "text" && settings.comet.privacyOnly && area != StorageArea.rejected
    ) {
      val pathsOutput = storageHandler
        .list(targetPath, ".txt", LocalDateTime.MIN, recursive = false)
        .filterNot(path => schema.pattern.matcher(path.getName).matches())
      if (pathsOutput.nonEmpty) {
        val txtPath = pathsOutput.head
        val finalTxtPath = new Path(
          targetPath,
          path.head.getName
        )
        storageHandler.move(txtPath, finalTxtPath)
      }
    }
    resultDataFrame
  }

  /** Main entry point as required by the Spark Job interface
    *
    * @return
    *   : Spark Session used for the job
    */
  def run(): Try[JobResult] = {
    val jobResult = domain.checkValidity(schemaHandler) match {
      case Left(errors) =>
        val errs = errors.reduce { (errs, err) =>
          errs + "\n" + err
        }
        Failure(throw new Exception(errs))
      case Right(_) =>
        val start = Timestamp.from(Instant.now())
        schema.presql.getOrElse(Nil).foreach(session.sql)
        val dataset = loadDataSet()
        dataset match {
          case Success(dataset) =>
            Try {
              val views = schemaHandler.views(domain.name)
              createSparkViews(views, schemaHandler.activeEnv ++ options)
              val (rejectedRDD, acceptedRDD) = ingest(dataset)
              val inputCount = dataset.count()
              val acceptedCount = acceptedRDD.count()
              val rejectedCount = rejectedRDD.count()
              val inputFiles = path.map(_.toString).mkString(",")
              logger.info(
                s"ingestion-summary -> files: [$inputFiles], domain: ${domain.name}, schema: ${schema.name}, input: $inputCount, accepted: $acceptedCount, rejected:$rejectedCount"
              )
              val end = Timestamp.from(Instant.now())
              val success = !settings.comet.rejectAllOnError || rejectedCount == 0
              val log = AuditLog(
                session.sparkContext.applicationId,
                inputFiles,
                domain.name,
                schema.name,
                success = success,
                inputCount,
                acceptedCount,
                rejectedCount,
                start,
                end.getTime - start.getTime,
                if (success) "success" else s"$rejectedCount invalid records",
                Step.LOAD.toString
              )
              AuditLog.sink(session, log)
              if (success) SparkJobResult(None)
              else throw new Exception("Fail on rejected count requested")
            }
          case Failure(exception) =>
            val end = Timestamp.from(Instant.now())
            val err = Utils.exceptionAsString(exception)
            val log = AuditLog(
              session.sparkContext.applicationId,
              path.map(_.toString).mkString(","),
              domain.name,
              schema.name,
              success = false,
              0,
              0,
              0,
              start,
              end.getTime - start.getTime,
              err,
              Step.LOAD.toString
            )
            AuditLog.sink(session, log)
            logger.error(err)
            Failure(throw exception)
        }
    }
    // After each ingestionjob we explicitely clear the spark cache
    session.catalog.clearCache()
    jobResult
  }

  private def computeNoTimestampFieldToMergeAndToDeleteDF(
    existingDF: DataFrame,
    mergeOptions: MergeOptions,
    finalIncomingDF: Dataset[Row]
  ): (DataFrame, DataFrame) = {
    val commonDF = existingDF
      .join(finalIncomingDF.select(mergeOptions.key.map(col): _*), mergeOptions.key)
      .select(finalIncomingDF.columns.map(col): _*)
    (commonDF, existingDF.except(commonDF).union(finalIncomingDF))
  }

  ///////////////////////////////////////////////////////////////////////////
  // Merge between the target and the source Dataframe
  ///////////////////////////////////////////////////////////////////////////

  private def processMerge(
    incomingDF: DataFrame,
    existingDF: DataFrame,
    mergeOptions: MergeOptions
  ): (DataFrame, Option[List[String]]) = {
    logger.info(s"incomingDF Schema before merge -> ${incomingDF.schema}")
    logger.info(s"existingDF Schema before merge -> ${existingDF.schema}")
    logger.info(s"existingDF field count=${existingDF.schema.fields.length}")
    logger.info(s"existingDF field list=${existingDF.schema.fields.map(_.name).mkString(",")}")
    logger.info(s"incomingDF field count=${incomingDF.schema.fields.length}")
    logger.info(s"incomingDF field list=${incomingDF.schema.fields.map(_.name).mkString(",")}")
    // We remove from the incoming data the records to delete
    val finalIncomingDF = mergeOptions.delete
      .map(condition => incomingDF.filter(s"not ($condition)"))
      .getOrElse(incomingDF)

    val (toDeleteDF, mergedDF) = mergeOptions.timestamp match {
      case Some(timestamp) =>
        // We only keep the first occurrence of each record, from both datasets
        computeToMergeAndToDeleteDF(existingDF, mergeOptions, finalIncomingDF, timestamp)
      case None =>
        // We directly remove from the existing dataset the row that are present in the incoming dataset
        computeNoTimestampFieldToMergeAndToDeleteDF(existingDF, mergeOptions, finalIncomingDF)
    }
    val partitionOverwriteMode =
      session.conf.get("spark.sql.sources.partitionOverwriteMode", "static").toLowerCase()
    val partitionsToUpdate = (
      partitionOverwriteMode,
      metadata.getSink(),
      settings.comet.mergeOptimizePartitionWrite
    ) match {
      // no need to apply optimization if existing dataset is empty
      case ("dynamic", Some(BigQuerySink(_, _, Some(timestamp), _, _, _, _)), true)
          if existingDF.limit(1).count() == 1 =>
        val partitionsToUpdate =
          computePartitionsToUpdateAfterMerge(finalIncomingDF, timestamp, toDeleteDF)
        Some(partitionsToUpdate)
      case ("static", _, _) | ("dynamic", _, _) =>
        None
      case (_, _, _) =>
        throw new Exception("Should never happen")
    }
    logger.whenDebugEnabled {
      logger.debug(s"Merge detected ${toDeleteDF.count()} items to update/delete")
      logger.debug(s"Merge detected ${finalIncomingDF.count()} items to update/insert")
      mergedDF.show(false)
    }
    if (settings.comet.mergeForceDistinct) (mergedDF.distinct(), partitionsToUpdate)
    else (mergedDF, partitionsToUpdate)
  }

  private def computeToMergeAndToDeleteDF(
    existingDF: DataFrame,
    merge: MergeOptions,
    finalIncomingDF: Dataset[Row],
    timestamp: String
  ): (DataFrame, DataFrame) = {
    val orderingWindow =
      Window.partitionBy(merge.key.head, merge.key.tail: _*).orderBy(col(timestamp).desc)

    val allRowsDF = existingDF.union(finalIncomingDF)

    val allRowsWithRownum = allRowsDF
      .withColumn("rownum", row_number.over(orderingWindow))

    // Deduplicate
    val mergedDF = allRowsWithRownum
      .where(col("rownum") === 1)
      .drop("rownum")

    // Compute rows that will be deleted (for logging purposes)
    val toDeleteDF = allRowsWithRownum
      .where(col("rownum") =!= 1)
      .drop("rownum")
    (toDeleteDF, mergedDF)
  }

  private def computePartitionsToUpdateAfterMerge(
    finalIncomingDF: Dataset[Row],
    timestamp: String,
    toDeleteDF: DataFrame
  ): List[String] = {
    logger.info(s"Computing partitions to update on date column $timestamp")
    val partitionsToUpdate = finalIncomingDF
      .select(col(timestamp))
      .union(toDeleteDF.select(col(timestamp)))
      .select(date_format(col(timestamp), "yyyyMMdd").cast("string"))
      .where(col(timestamp).isNotNull)
      .distinct()
      .collect()
      .map(_.getString(0))
      .toList
    logger.info(
      s"The following partitions will be updated ${partitionsToUpdate.mkString(",")}"
    )
    partitionsToUpdate
  }

  private def mergeFromParquet(
    acceptedPath: Path,
    withScriptFieldsDF: DataFrame,
    mergeOptions: MergeOptions
  ): DataFrame = {
    if (storageHandler.exists(new Path(acceptedPath, "_SUCCESS"))) {
      // Otherwise load from accepted area
      // We provide the accepted DF schema since partition columns types are infered when parquet is loaded and might not match with the DF being ingested
      val existingDF =
        session.read.schema(withScriptFieldsDF.schema).parquet(acceptedPath.toString)
      if (
        existingDF.schema.fields.length == session.read
          .parquet(acceptedPath.toString)
          .schema
          .fields
          .length
      )
        applyMergeParquet(withScriptFieldsDF, existingDF, mergeOptions)
      else
        throw new RuntimeException(
          "Input Dataset and existing HDFS dataset do not have the same number of columns. Check for changes in the dataset schema ?"
        )
    } else {
      val emptyExistingDF = session
        .createDataFrame(session.sparkContext.emptyRDD[Row], withScriptFieldsDF.schema)
      applyMergeParquet(withScriptFieldsDF, emptyExistingDF, mergeOptions)
    }
  }

  /** Merge incoming and existing dataframes using merge options
    *
    * @param inputDF
    * @param existingDF
    * @param mergeOptions
    * @return
    *   merged dataframe
    */
  private def applyMergeParquet(
    inputDF: DataFrame,
    existingDF: DataFrame,
    mergeOptions: MergeOptions
  ): DataFrame = {

    val partitionedInputDF = partitionDataset(inputDF, metadata.getPartitionAttributes())
    logger.info(s"partitionedInputDF field count=${partitionedInputDF.schema.fields.length}")
    logger.info(s"""partitionedInputDF field list=${partitionedInputDF.schema.fields
      .map(_.name)
      .mkString(",")}""")

    // Force ordering of columns to be the same
    val orderedExisting =
      existingDF.select(partitionedInputDF.columns.map(col): _*)

    val (dataframe, _) = processMerge(partitionedInputDF, orderedExisting, mergeOptions)
    dataframe
  }

  /** Spark BigQuery driver consider interger in BQ as Long. We need to convert the Int DataType to
    * LongType before loading the data. As a good practice, always use long when dealing with big
    * query in your YAML Schema.
    * @param schema
    * @return
    */
  private def converIntToLongInSchema(schema: StructType) = {
    // TODO: Support complex fields
    val fields = schema.fields.map { field =>
      field.dataType match {
        case IntegerType => field.copy(dataType = LongType)
        case StructType(_) | ArrayType(_, _) =>
          logger.warn(
            "nested and repeated fields conversion not supported in converIntToLongInSchema for BigQuery"
          )
          field
        case _ => field
      }
    }
    schema.copy(fields = fields)
  }
  ///////////////////////////////////////////////////////////////////////////
  // Merge From BigQuery Data Source
  ///////////////////////////////////////////////////////////////////////////

  /** In the queryFilter, the user may now write something like this : `partitionField in last(3)`
    * this will be translated to partitionField between partitionStart and partitionEnd
    *
    * partitionEnd is the last partition in the dataset paritionStart is the 3rd last partition in
    * the dataset
    *
    * if partititionStart or partitionEnd does nos exist (aka empty dataset) they are replaced by
    * 19700101
    *
    * @param withScriptFieldsDF
    * @param mergeOptions
    * @return
    */

  private def mergeFromBQ(
    withScriptFieldsDF: DataFrame,
    mergeOptions: MergeOptions
  ): (DataFrame, Option[List[String]]) = {
    // When merging to BigQuery, load existing DF from BigQuery
    val tableMetadata = BigQuerySparkJob.getTable(session, domain.name, schema.name)
    tableMetadata.table
      .map { table =>
        if (
          table.getDefinition
            .asInstanceOf[StandardTableDefinition]
            .getSchema
            .getFields
            .size() == withScriptFieldsDF.schema.fields.length
        ) {
          val bqTable = s"${domain.name}.${schema.name}"
          // We provided the acceptedDF schema here since BQ lose the required / nullable information of the schema
          val existingBQDFWithoutFilter = session.read
            .schema(converIntToLongInSchema(withScriptFieldsDF.schema))
            .format("com.google.cloud.spark.bigquery")
            .option("table", bqTable)

          val existingBigQueryDFReader = (mergeOptions.queryFilter, metadata.sink) match {
            case (Some(_), Some(BigQuerySink(_, _, Some(_), _, _, _, _))) =>
              val partitions =
                tableMetadata.biqueryClient.listPartitions(table.getTableId).asScala.toList
              val filter = mergeOptions.buidlBQQuery(partitions, options)
              existingBQDFWithoutFilter
                .option("filter", filter.getOrElse(throw new Exception("should never happen")))
            case (_, _) =>
              existingBQDFWithoutFilter
          }
          processMerge(withScriptFieldsDF, existingBigQueryDFReader.load(), mergeOptions)
        } else
          throw new RuntimeException(
            "Input Dataset and existing HDFS dataset do not have the same number of columns. Check for changes in the dataset schema ?"
          )
      }
      .getOrElse {
        val emptyExistingDF = session
          .createDataFrame(session.sparkContext.emptyRDD[Row], withScriptFieldsDF.schema)
        processMerge(withScriptFieldsDF, emptyExistingDF, mergeOptions)
      }
  }

  def reorderAttributes(dataFrame: DataFrame): List[Attribute] = {
    val finalSchema = schema.attributesWithoutScript :+ Attribute(
      name = Settings.cometInputFileNameColumn
    )
    val attributesMap =
      finalSchema.map(attr => (attr.name, attr)).toMap
    dataFrame.columns.map(colName => attributesMap(colName)).toList
  }
}

object IngestionUtil {

  val rejectedCols = List(
    ("jobid", LegacySQLTypeName.STRING, StringType),
    ("timestamp", LegacySQLTypeName.TIMESTAMP, TimestampType),
    ("domain", LegacySQLTypeName.STRING, StringType),
    ("schema", LegacySQLTypeName.STRING, StringType),
    ("error", LegacySQLTypeName.STRING, StringType),
    ("path", LegacySQLTypeName.STRING, StringType)
  )

  private def bigqueryRejectedSchema(): BQSchema = {
    val fields = rejectedCols map { attribute =>
      Field
        .newBuilder(attribute._1, attribute._2)
        .setMode(Field.Mode.REQUIRED)
        .setDescription("")
        .build()
    }
    BQSchema.of(fields: _*)
  }

  def sinkRejected(
    session: SparkSession,
    rejectedRDD: RDD[String],
    domainName: String,
    schemaName: String,
    now: Timestamp
  )(implicit settings: Settings): Try[(Dataset[Row], Path)] = {
    import session.implicits._
    val rejectedPath = new Path(DatasetArea.rejected(domainName), schemaName)
    val rejectedPathName = rejectedPath.toString
    // We need to save first the application ID
    // refrencing it inside the worker (rdd.map) below would fail.
    val applicationId = session.sparkContext.applicationId
    val rejectedTypedRDD = rejectedRDD.map { err =>
      RejectedRecord(
        applicationId,
        now,
        domainName,
        schemaName,
        err,
        rejectedPathName
      )
    }
    val rejectedDF = session
      .createDataFrame(
        rejectedTypedRDD.toDF().rdd,
        StructType(
          rejectedCols.map { case (attrName, _, sparkType) =>
            StructField(attrName, sparkType, nullable = false)
          }
        )
      )
      .toDF(rejectedCols.map { case (attrName, _, _) => attrName }: _*)
      .limit(settings.comet.audit.maxErrors)

    val res =
      settings.comet.audit.sink match {
        case sink: BigQuerySink =>
          val bqConfig = BigQueryLoadConfig(
            Right(rejectedDF),
            outputDataset = sink.name.getOrElse("audit"),
            outputTable = "rejected",
            None,
            Nil,
            "parquet",
            "CREATE_IF_NEEDED",
            "WRITE_APPEND",
            None,
            None,
            options = sink.getOptions
          )
          new BigQuerySparkJob(bqConfig, Some(bigqueryRejectedSchema())).run()

        case sink: JdbcSink =>
          val jdbcConfig = ConnectionLoadConfig.fromComet(
            sink.connection,
            settings.comet,
            Right(rejectedDF),
            "rejected",
            partitions = sink.partitions.getOrElse(1),
            batchSize = sink.batchsize.getOrElse(1000),
            options = sink.getOptions
          )

          new ConnectionLoadJob(jdbcConfig).run()

        case _: EsSink =>
          // TODO Sink Rejected Log to ES
          throw new Exception("Sinking Audit log to Elasticsearch not yet supported")
        case _: NoneSink | FsSink(_, _, _, _) =>
          // We save in the caller
          // TODO rewrite this one
          Success(())
      }
    res match {
      case Success(_) => Success(rejectedDF, rejectedPath)
      case Failure(e) => Failure(e)
    }
  }

  def validateCol(
    colRawValue: Option[String],
    colAttribute: Attribute,
    tpe: Type,
    colMap: => Map[String, Option[String]]
  )(
    implicit /* TODO: make me explicit. Avoid rebuilding the PrivacyLevel(settings) at each invocation? */ settings: Settings
  ): ColResult = {
    def ltrim(s: String) = s.replaceAll("^\\s+", "")

    def rtrim(s: String) = s.replaceAll("\\s+$", "")

    val trimmedColValue = colRawValue.map { colRawValue =>
      colAttribute.trim match {
        case Some(LEFT)  => ltrim(colRawValue)
        case Some(RIGHT) => rtrim(colRawValue)
        case Some(BOTH)  => colRawValue.trim()
        case _           => colRawValue
      }
    }

    val colValue = trimmedColValue
      .map { trimmedColValue =>
        if (trimmedColValue.isEmpty) colAttribute.default.getOrElse("")
        else
          trimmedColValue
      }
      .orElse(colAttribute.default)

    def colValueIsNullOrEmpty = colValue match {
      case None           => true
      case Some(colValue) => colValue.isEmpty
    }

    def optionalColIsEmpty = !colAttribute.required && colValueIsNullOrEmpty

    def requiredColIsEmpty = colAttribute.required && colValueIsNullOrEmpty

    def colPatternIsValid = colValue.exists(tpe.matches)

    val privacyLevel = colAttribute.getPrivacy()
    val privacy = colValue.map { colValue =>
      if (privacyLevel == PrivacyLevel.None)
        colValue
      else
        privacyLevel.crypt(colValue, colMap)
    }

    val colPatternOK = !requiredColIsEmpty && (optionalColIsEmpty || colPatternIsValid)

    val (sparkValue, colParseOK) = {
      (colPatternOK, privacy) match {
        case (false, _) =>
          (None, false)
        case (true, None) =>
          (None, true)
        case (true, Some(privacy)) =>
          Try(tpe.sparkValue(privacy)) match {
            case Success(res) => (Some(res), true)
            case Failure(_)   => (None, false)
          }
      }
    }
    ColResult(
      ColInfo(
        colValue,
        colAttribute.name,
        tpe.name,
        tpe.pattern,
        colPatternOK && colParseOK
      ),
      sparkValue.orNull
    )
  }
}

object ImprovedDataFrameContext {

  implicit class ImprovedDataFrame(df: org.apache.spark.sql.DataFrame) {

    def T(query: String): org.apache.spark.sql.DataFrame = {
      new SQLTransformer().setStatement(query).transform(df)
    }
  }

}
