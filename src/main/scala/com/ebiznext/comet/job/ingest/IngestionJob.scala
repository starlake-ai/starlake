package com.ebiznext.comet.job.ingest

import com.ebiznext.comet.config.{DatasetArea, Settings, StorageArea}
import com.ebiznext.comet.job.index.bqload.{BigQueryLoadConfig, BigQuerySparkJob}
import com.ebiznext.comet.job.index.connectionload.{ConnectionLoadConfig, ConnectionLoadJob}
import com.ebiznext.comet.job.index.esload.{ESLoadConfig, ESLoadJob}
import com.ebiznext.comet.job.ingest.ImprovedDataFrameContext._
import com.ebiznext.comet.job.metrics.{AssertionJob, MetricsJob}
import com.ebiznext.comet.schema.handlers.{SchemaHandler, StorageHandler}
import com.ebiznext.comet.schema.model.Rejection.{ColInfo, ColResult}
import com.ebiznext.comet.schema.model.Trim.{BOTH, LEFT, RIGHT}
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.Formatter._
import com.ebiznext.comet.utils.kafka.KafkaClient
import com.ebiznext.comet.utils.{JobResult, SparkJob, SparkJobResult, Utils}
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.{Field, LegacySQLTypeName, StandardTableDefinition}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}
import scala.collection.JavaConverters._
import scala.language.existentials
import scala.util.{Failure, Success, Try}

/**
  */
trait IngestionJob extends SparkJob {

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

  /** Dataset loading strategy (JSON / CSV / ...)
    *
    * @return Spark Dataframe loaded using metadata options
    */
  protected def loadDataSet(): Try[DataFrame]

  /** ingestion algorithm
    *
    * @param dataset
    */
  protected def ingest(dataset: DataFrame): (RDD[_], RDD[_])

  protected def applyIgnore(dfIn: DataFrame): Dataset[Row] = {
    import org.apache.spark.sql.functions._
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

  protected def saveRejected(rejectedRDD: RDD[String]): Try[Path] = {
    logger.whenDebugEnabled {
      logger.debug(s"rejectedRDD SIZE ${rejectedRDD.count()}")
      rejectedRDD.take(100).foreach(rejected => logger.debug(rejected.replaceAll("\n", "|")))
    }
    val domainName = domain.name
    val schemaName = schema.name
    val start = Timestamp.from(Instant.now())
    IngestionUtil.sinkRejected(session, rejectedRDD, domainName, schemaName, now) match {
      case Success((rejectedDF, rejectedPath)) =>
        if (settings.comet.sinkToFile)
          sinkToFile(
            rejectedDF,
            rejectedPath,
            WriteMode.APPEND,
            StorageArea.rejected,
            merge = false,
            settings.comet.defaultRejectedWriteFormat
          )
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
        SparkAuditLogWriter.append(session, log)
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
        SparkAuditLogWriter.append(session, log)
        Failure(exception)
    }
  }

  def getWriteMode(): WriteMode =
    schema.merge
      .map(_ => WriteMode.OVERWRITE)
      .getOrElse(metadata.getWrite())

  private def csvOutput(): Boolean =
    settings.comet.csvOutput && !settings.comet.grouped && metadata.partition.isEmpty && path.nonEmpty

  /** Merge new and existing dataset if required
    * Save using overwrite / Append mode
    *
    * @param acceptedDF
    */
  protected def saveAccepted(acceptedDF: DataFrame): (DataFrame, Path) = {
    val start = Timestamp.from(Instant.now())
    logger.whenDebugEnabled {
      logger.debug(s"acceptedRDD SIZE ${acceptedDF.count()}")
      acceptedDF.show(1000)
    }

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
      )
        .run()
        .get
    }

    if (settings.comet.metrics.active) {
      new MetricsJob(this.domain, this.schema, Stage.UNIT, this.storageHandler, this.schemaHandler)
        .run(acceptedDF, System.currentTimeMillis())
        .get
    }

    val writeMode = getWriteMode()
    val acceptedPath = new Path(DatasetArea.accepted(domain.name), schema.name)

    val acceptedDfWithScriptFields = (if (schema.attributes.exists(_.script.isDefined)) {
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
                                                  Some(script)
                                                )
                                              ) =>
                                            df.T(
                                              s"SELECT *, ${script.richFormat(options)} as $name FROM __THIS__"
                                            )
                                          case (df, _) => df
                                        }
                                      } else acceptedDF).drop(Settings.cometInputFileNameColumn)

    val finalAcceptedDF: DataFrame = schema.attributes.exists(_.script.isDefined) match {
      case true => {
        logger.whenDebugEnabled {
          logger.debug("Accepted Dataframe schema right after adding computed columns")
          acceptedDfWithScriptFields.printSchema()
        }
        // adding computed columns can change the order of columns, we must force the order defined in the schema
        val orderedWithScriptFieldsDF =
          session.createDataFrame(
            acceptedDfWithScriptFields.rdd,
            schema.sparkTypeWithRenamedFields(schemaHandler)
          )
        logger.whenDebugEnabled {
          logger.debug("Accepted Dataframe schema after applying the defined schema")
          orderedWithScriptFieldsDF.printSchema()
        }
        orderedWithScriptFieldsDF
      }
      case false => acceptedDfWithScriptFields
    }

    val mergedDF = schema.merge
      .map { mergeOptions =>
        if (metadata.getSink().map(_.`type`).getOrElse(SinkType.None) == SinkType.BQ) {
          val mergedDfBq = mergeFromBQ(finalAcceptedDF, mergeOptions)
          mergedDfBq
        } else {
          mergeFromParquet(acceptedPath, finalAcceptedDF, mergeOptions)
        }
      }
      .getOrElse(finalAcceptedDF)

    logger.info("Merged Dataframe Schema")
    mergedDF.printSchema()
    val savedDataset =
      if (settings.comet.sinkToFile)
        sinkToFile(
          mergedDF,
          acceptedPath,
          writeMode,
          StorageArea.accepted,
          schema.merge.isDefined,
          settings.comet.defaultWriteFormat
        )
      else
        mergedDF
    logger.info("Saved Dataset Schema")
    savedDataset.printSchema()
    sink(mergedDF) match {
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
        SparkAuditLogWriter.append(session, log)
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
        SparkAuditLogWriter.append(session, log)
    }
    (savedDataset, acceptedPath)
  }

  private[this] def sink(mergedDF: DataFrame): Try[Unit] = {
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
          val tableSchema = schema.mergedMetadata(domain.metadata).getFormat() match {
            case Format.XML => None
            case _          => Some(schema.bqSchema(schemaHandler))
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
            options = sink.map(_.getOptions).getOrElse(Map.empty)
          )
          val res = new BigQuerySparkJob(config, tableSchema).run()
          res match {
            case Success(_) => ;
            case Failure(e) =>
              throw e
          }

        case SinkType.KAFKA =>
          Utils.withResources(new KafkaClient(settings.comet.kafka)) { client =>
            client.sinkToTopic(schema.name, settings.comet.kafka.topics(schema.name), mergedDF)
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
        case SinkType.None =>
          // ignore
          logger.trace("not producing an index, as requested (no sink or sink at None explicitly)")
      }
    }
  }

  /** Save typed dataset in parquet. If hive support is active, also register it as a Hive Table and if analyze is active, also compute basic statistics
    *
    * @param dataset    : dataset to save
    * @param targetPath : absolute path
    * @param writeMode  : Append or overwrite
    * @param area       : accepted or rejected area
    */
  private[this] def sinkToFile(
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
        try {
          session.sql(s"drop table if exists $hiveDB.$tableName")
        } catch {
          case e: Exception =>
            logger.warn("Ignore error when hdfs files not found")
            Utils.logException(logger, e)

        }
      }

      val tmpPath = new Path(s"${targetPath.toString}.tmp")

      val nbPartitions = metadata.getSamplingStrategy() match {
        case 0.0 => // default partitioning
          if (csvOutput())
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
        if (
          area == StorageArea.rejected && !metadata
            .getPartitionAttributes()
            .forall(Metadata.CometPartitionColumns.contains(_))
        )
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
          .format(settings.comet.defaultWriteFormat)
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
        if (csvOutput() && area != StorageArea.rejected)
          targetDatasetWriter
            .mode(saveMode)
            .format("csv")
            .option("ignoreLeadingWhiteSpace", value = false)
            .option("ignoreTrailingWhiteSpace", value = false)
            .option("header", metadata.withHeader.getOrElse(false))
            .option("delimiter", metadata.separator.getOrElse("µ"))
            .option("path", targetPath.toString)
        else
          targetDatasetWriter
            .mode(saveMode)
            .format(writeFormat)
            .option("path", targetPath.toString)

      logger.info(s"Saving Dataset to final location $targetPath")
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
        val finalCsvPath = new Path(
          targetPath,
          path.head.getName
        )
        storageHandler.move(csvPath, finalCsvPath)
      }
    }
    resultDataFrame
  }

  /** Main entry point as required by the Spark Job interface
    *
    * @return : Spark Session used for the job
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
              createViews(views, options, schemaHandler.activeEnv)
              val (rejectedRDD, acceptedRDD) = ingest(dataset)
              val inputCount = dataset.count()
              val acceptedCount = acceptedRDD.count()
              val rejectedCount = rejectedRDD.count()
              val inputFiles = dataset.inputFiles.mkString(",")
              logger.info(
                s"ingestion-summary -> files: [$inputFiles], domain: ${domain.name}, schema: ${schema.name}, input: $inputCount, accepted: $acceptedCount, rejected:$rejectedCount"
              )
              val end = Timestamp.from(Instant.now())
              val log = AuditLog(
                session.sparkContext.applicationId,
                inputFiles,
                domain.name,
                schema.name,
                success = true,
                inputCount,
                acceptedCount,
                rejectedCount,
                start,
                end.getTime - start.getTime,
                "success",
                Step.LOAD.toString
              )
              SparkAuditLogWriter.append(session, log)
              schema.postsql.getOrElse(Nil).foreach(session.sql)
              SparkJobResult(None)
            }
          case Failure(exception) =>
            val end = Timestamp.from(Instant.now())
            val err = Utils.exceptionAsString(exception)
            AuditLog(
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
            logger.error(err)
            Failure(throw exception)
        }
    }
    // After each ingestionjob we explicitely clear the spark cache
    session.catalog.clearCache()
    jobResult
  }

  ///////////////////////////////////////////////////////////////////////////
  // Merge between the target and the source Dataframe
  ///////////////////////////////////////////////////////////////////////////

  private[this] def processMerge(in: DataFrame, existing: DataFrame, merge: MergeOptions) = {
    logger.info(s"inputDF Schema before merge -> ${in.schema}")
    logger.info(s"existingDF Schema before merge -> ${existing.schema}")
    logger.info(s"existingDF field count=${existing.schema.fields.length}")
    logger.info(s"""existingDF field list=${existing.schema.fields.map(_.name).mkString(",")}""")
    logger.info(s"inputDF field count=${in.schema.fields.length}")
    logger.info(s"""inputDF field list=${in.schema.fields.map(_.name).mkString(",")}""")

    // Force ordering again of columns to be the same since join operation change it otherwise except below won"'t work.
    val commonDF =
      existing
        .join(in.select(merge.key.head, merge.key.tail: _*), merge.key)
        .select(in.columns.map(col): _*)

    val toDeleteDF = merge.timestamp.map { timestamp =>
      val w =
        Window.partitionBy(merge.key.head, merge.key.tail: _*).orderBy(col(timestamp).desc)
      import org.apache.spark.sql.functions.row_number
      commonDF
        .withColumn("rownum", row_number.over(w))
        .where(col("rownum") =!= 1)
        .drop("rownum")
    } getOrElse commonDF

    val updatesDF = merge.delete
      .map(condition => in.filter(s"not ($condition)"))
      .getOrElse(in)
    logger.whenDebugEnabled {
      logger.debug(s"Merge detected ${toDeleteDF.count()} items to update/delete")
      logger.debug(s"Merge detected ${updatesDF.count()} items to update/insert")
      existing.except(toDeleteDF).union(updatesDF).show(false)
    }
    if (settings.comet.mergeForceDistinct) {
      existing
        .except(toDeleteDF)
        .union(updatesDF)
        .distinct()
    } else {
      existing.except(toDeleteDF).union(updatesDF)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Merge From Parquets Data Source
  ///////////////////////////////////////////////////////////////////////////

  private[this] def mergeFromParquet(
    acceptedPath: Path,
    withScriptFieldsDF: DataFrame,
    mergeOptions: MergeOptions
  ) = {
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
        mergeParquet(withScriptFieldsDF, existingDF, mergeOptions)
      else
        throw new RuntimeException(
          "Input Dataset and existing HDFS dataset do not have the same number of columns. Check for changes in the dataset schema ?"
        )
      mergeParquet(withScriptFieldsDF, existingDF, mergeOptions)
    } else {
      withScriptFieldsDF
    }

  }

  /** Merge incoming and existing dataframes using merge options
    *
    * @param inputDF
    * @param existingDF
    * @param merge
    * @return merged dataframe
    */
  private[this] def mergeParquet(
    inputDF: DataFrame,
    existingDF: DataFrame,
    merge: MergeOptions
  ): DataFrame = {

    val partitionedInputDF = partitionDataset(inputDF, metadata.getPartitionAttributes())
    logger.info(s"partitionedInputDF field count=${partitionedInputDF.schema.fields.length}")
    logger.info(s"""partitionedInputDF field list=${partitionedInputDF.schema.fields
      .map(_.name)
      .mkString(",")}""")

    // Force ordering of columns to be the same
    val orderedExisting =
      existingDF.select(partitionedInputDF.columns.map(col): _*)

    processMerge(partitionedInputDF, orderedExisting, merge)

  }

  ///////////////////////////////////////////////////////////////////////////
  // Merge From BigQuery Data Source
  ///////////////////////////////////////////////////////////////////////////

  private[this] def mergeFromBQ(withScriptFieldsDF: DataFrame, mergeOptions: MergeOptions) = {
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
          (mergeOptions.queryFilter, metadata.sink) match {
            case (Some(query), Some(BigQuerySink(_, _, Some(_), _, _, _, _))) =>
              val queryArgs = query.richFormat(options)
              if (queryArgs.contains("latest")) {
                val partitions =
                  tableMetadata.biqueryClient.listPartitions(table.getTableId).asScala.toList
                val latestPartition = partitions.last
                val existingBigQueryDF = session.read
                  // We provided the acceptedDF schema here since BQ lose the required / nullable information of the schema
                  .schema(withScriptFieldsDF.schema)
                  .format("com.google.cloud.spark.bigquery")
                  .option("table", bqTable)
                  .option(
                    "filter",
                    queryArgs.replace("latest", s"PARSE_DATE('%Y%m%d','$latestPartition')")
                  )
                  .load()
                processMerge(withScriptFieldsDF, existingBigQueryDF, mergeOptions)

              } else {
                val existingBigQueryDF = session.read
                  // We provided the acceptedDF schema here since BQ lose the required / nullable information of the schema
                  .schema(withScriptFieldsDF.schema)
                  .format("com.google.cloud.spark.bigquery")
                  .option("table", bqTable)
                  .option("filter", queryArgs)
                  .load()
                processMerge(withScriptFieldsDF, existingBigQueryDF, mergeOptions)
              }
            case _ =>
              val existingBigQueryDF = session.read
                // We provided the acceptedDF schema here since BQ lose the required / nullable information of the schema
                .schema(withScriptFieldsDF.schema)
                .format("com.google.cloud.spark.bigquery")
                .option("table", bqTable)
                .load()
              processMerge(withScriptFieldsDF, existingBigQueryDF, mergeOptions)
          }

        } else
          throw new RuntimeException(
            "Input Dataset and existing HDFS dataset do not have the same number of columns. Check for changes in the dataset schema ?"
          )
      }
      .getOrElse(withScriptFieldsDF)
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

  import com.google.cloud.bigquery.{Schema => BQSchema}

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
          rejectedCols.map(col => StructField(col._1, col._3, nullable = false))
        )
      )
      .toDF(rejectedCols.map(_._1): _*)
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
          ???
        case _: NoneSink =>
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

    val colValue = trimmedColValue.map { trimmedColValue =>
      if (trimmedColValue.isEmpty) colAttribute.default.getOrElse("")
      else trimmedColValue
    }

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

  import org.apache.spark.ml.feature.SQLTransformer

  implicit class ImprovedDataFrame(df: org.apache.spark.sql.DataFrame) {

    def T(query: String): org.apache.spark.sql.DataFrame = {
      new SQLTransformer().setStatement(query).transform(df)
    }
  }

}
