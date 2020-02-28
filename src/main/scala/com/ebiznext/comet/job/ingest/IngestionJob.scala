package com.ebiznext.comet.job.ingest

import java.sql.Timestamp
import java.time.Instant

import com.ebiznext.comet.config.Settings.IndexSinkSettings
import com.ebiznext.comet.config.{DatasetArea, Settings, StorageArea}
import com.ebiznext.comet.job.bqload.{BigQueryLoadConfig, BigQueryLoadJob}
import com.ebiznext.comet.job.index.{IndexConfig, IndexJob}
import com.ebiznext.comet.job.jdbcload.{JdbcLoadConfig, JdbcLoadJob}
import com.ebiznext.comet.job.metrics.MetricsJob
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.Rejection.{ColInfo, ColResult}
import com.ebiznext.comet.schema.model.Trim.{BOTH, LEFT, RIGHT}
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.{SparkJob, Utils}
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.{Field, LegacySQLTypeName}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import scala.language.existentials
import scala.util.{Failure, Success, Try}

/**
  *
  */
trait IngestionJob extends SparkJob {
  def domain: Domain

  def schema: Schema

  def storageHandler: StorageHandler

  def types: List[Type]

  def path: List[Path]

  val now: Timestamp = java.sql.Timestamp.from(Instant.now)

  /**
    * Merged metadata
    */
  lazy val metadata: Metadata = schema.mergedMetadata(domain.metadata)

  /**
    * Dataset loading strategy (JSON / CSV / ...)
    *
    * @return Spark Dataframe loaded using metadata options
    */
  def loadDataSet(): Try[DataFrame]

  /**
    * ingestion algorithm
    *
    * @param dataset
    */
  def ingest(dataset: DataFrame): (RDD[_], RDD[_])

  def saveRejected(rejectedRDD: RDD[String]): Try[Path] = {
    logger.info(s"rejectedRDD SIZE ${rejectedRDD.count()}")
    rejectedRDD.take(100).foreach(rejected => logger.info(rejected.replaceAll("\n", "|")))
    val writeMode = WriteMode.APPEND
    import session.implicits._
    logger.whenDebugEnabled {
      rejectedRDD.toDF.show(100, false)
    }
    val domainName = domain.name
    val schemaName = schema.name
    IngestionUtil.saveRejected(session, rejectedRDD, domainName, schemaName.toString, now) match {
      case Success((rejectedDF, rejectedPath)) =>
        (rejectedDF, rejectedPath)
        saveRows(rejectedDF, rejectedPath, WriteMode.APPEND, StorageArea.rejected, false)
        Success(rejectedPath)
      case Failure(exception) =>
        logger.error("Failed to save Rejected", exception)
        Failure(exception)
    }
  }

  def getWriteMode(): WriteMode =
    schema.merge
      .map(_ => WriteMode.OVERWRITE)
      .getOrElse(metadata.getWriteMode())

  /**
    * Merge new and existing dataset if required
    * Save using overwrite / Append mode
    *
    * @param acceptedDF
    */
  def saveAccepted(acceptedDF: DataFrame): (DataFrame, Path) = {
    logger.whenDebugEnabled {
      logger.debug(s"acceptedRDD SIZE ${acceptedDF.count()}")
      acceptedDF.show(1000)
    }
    if (settings.comet.metrics.active) {
      new MetricsJob(this.domain, this.schema, Stage.UNIT, this.storageHandler)
        .run(acceptedDF, System.currentTimeMillis())
    }
    val writeMode = getWriteMode()
    val acceptedPath = new Path(DatasetArea.accepted(domain.name), schema.name)
    val mergedDF = schema.merge.map { mergeOptions =>
      if (storageHandler.exists(new Path(acceptedPath, "_SUCCESS"))) {
        val existingDF = session.read.parquet(acceptedPath.toString)
        merge(acceptedDF, existingDF, mergeOptions)
      } else
        acceptedDF
    } getOrElse (acceptedDF)

    saveRows(mergedDF, acceptedPath, writeMode, StorageArea.accepted, schema.merge.isDefined)

    if (settings.comet.metrics.active) {
      new MetricsJob(this.domain, this.schema, Stage.GLOBAL, storageHandler).run()
    }
    (mergedDF, acceptedPath)
  }

  def index(mergedDF: DataFrame): Unit = {
    val meta = schema.mergedMetadata(domain.metadata)
    meta.getIndexSink() match {
      case Some(IndexSink.ES) if settings.comet.elasticsearch.active =>
        val properties = meta.properties
        val config = IndexConfig(
          timestamp = properties.flatMap(_.get("timestamp")),
          id = properties.flatMap(_.get("id")),
          format = "parquet",
          domain = domain.name,
          schema = schema.name
        )
        new IndexJob(config, settings.storageHandler).run()
      case Some(IndexSink.BQ) =>
        val (createDisposition: String, writeDisposition: String) = Utils.getDBDisposition(
          meta.getWriteMode()
        )
        val config = BigQueryLoadConfig(
          sourceFile = Right(mergedDF),
          outputTable = schema.name,
          outputDataset = domain.name,
          sourceFormat = "parquet",
          createDisposition = createDisposition,
          writeDisposition = writeDisposition,
          location = meta.getProperties().get("location"),
          outputPartition = meta.getProperties().get("timestamp"),
          days = meta.getProperties().get("days").map(_.toInt)
        )
        val res = new BigQueryLoadJob(config).run()
        res match {
          case Success(_) => ;
          case Failure(e) => logger.error("BQLoad Failed", e)
        }

      case Some(IndexSink.JDBC) =>
        val (createDisposition: CreateDisposition, writeDisposition: WriteDisposition) = {

          val (cd, wd) = Utils.getDBDisposition(
            meta.getWriteMode()
          )
          (CreateDisposition.valueOf(cd), WriteDisposition.valueOf(wd))
        }
        meta.getProperties().get("jdbc").foreach { jdbcName =>
          val partitions = meta.getProperties().getOrElse("partitions", "1").toInt
          val batchSize = meta.getProperties().getOrElse("batchsize", "1000").toInt

          val jdbcConfig = JdbcLoadConfig.fromComet(
            jdbcName,
            settings.comet,
            Right(mergedDF),
            schema.name,
            createDisposition = createDisposition,
            writeDisposition = writeDisposition,
            partitions = partitions,
            batchSize = batchSize
          )

          val res = new JdbcLoadJob(jdbcConfig).run()
          res match {
            case Success(_) => ;
            case Failure(e) => logger.error("JDBCLoad Failed", e)
          }
        }

      case _ =>
      // ignore
    }
  }

  /**
    * Merge incoming and existing dataframes using merge options
    *
    * @param inputDF
    * @param existingDF
    * @param merge
    * @return merged dataframe
    */
  def merge(inputDF: DataFrame, existingDF: DataFrame, merge: MergeOptions): DataFrame = {
    logger.info(s"existingDF field count=${existingDF.schema.fields.length}")
    logger.info(s"""existingDF field list=${existingDF.schema.fields.map(_.name).mkString(",")}""")
    logger.info(s"inputDF field count=${inputDF.schema.fields.length}")
    logger.info(s"""inputDF field list=${inputDF.schema.fields.map(_.name).mkString(",")}""")

    val partitionedInputDF = partitionDataset(inputDF, metadata.getPartitionAttributes())
    logger.info(s"partitionedInputDF field count=${partitionedInputDF.schema.fields.length}")
    logger.info(s"""partitionedInputDF field list=${partitionedInputDF.schema.fields
      .map(_.name)
      .mkString(",")}""")

    if (existingDF.schema.fields.length != partitionedInputDF.schema.fields.length) {
      throw new RuntimeException(
        "Input Dataset and existing HDFS dataset do not have the same number of columns. Check for changes in the dataset schema ?"
      )
    }

    // Force ordering of columns to be the same
    val orderedExisting = existingDF.select(partitionedInputDF.columns.map((col(_))): _*)

    // Force ordering again of columns to be the same since join operation change it otherwise except below won"'t work.
    val commonDF =
      orderedExisting
        .join(partitionedInputDF.select(merge.key.head, merge.key.tail: _*), merge.key)
        .select(partitionedInputDF.columns.map((col(_))): _*)

    val toDeleteDF = merge.timestamp.map { timestamp =>
      val w = Window.partitionBy(merge.key.head, merge.key.tail: _*).orderBy(col(timestamp).desc)
      import org.apache.spark.sql.functions.row_number
      commonDF
        .withColumn("rownum", row_number.over(w))
        .where(col("rownum") =!= 1)
        .drop("rownum")
    } getOrElse (commonDF)

    val updatesDF = merge.delete
      .map(condition => partitionedInputDF.filter(s"not ($condition)"))
      .getOrElse(partitionedInputDF)
    logger.whenDebugEnabled {
      logger.debug(s"Merge detected ${toDeleteDF.count()} items to update/delete")
      logger.debug(s"Merge detected ${updatesDF.count()} items to update/insert")
      orderedExisting.except(toDeleteDF).union(updatesDF).show(false)
    }
    if (settings.comet.mergeForceDistinct)
      orderedExisting.except(toDeleteDF).union(updatesDF).distinct()
    else
      orderedExisting.except(toDeleteDF).union(updatesDF)
  }

  /**
    * Save typed dataset in parquet. If hive support is active, also register it as a Hive Table and if analyze is active, also compute basic statistics
    *
    * @param dataset    : dataset to save
    * @param targetPath : absolute path
    * @param writeMode  : Append or overwrite
    * @param area       : accepted or rejected area
    */
  def saveRows(
    dataset: DataFrame,
    targetPath: Path,
    writeMode: WriteMode,
    area: StorageArea,
    merge: Boolean
  ): (DataFrameWriter[Row], String) = {
    if (dataset.columns.length > 0) {
      val count = dataset.count()
      val saveMode = writeMode.toSaveMode
      val hiveDB = StorageArea.area(domain.name, area)
      val tableName = schema.name
      val fullTableName = s"$hiveDB.$tableName"
      if (settings.comet.hive) {
        logger.info(
          s"DSV Output $count records to Hive table $hiveDB/$tableName($saveMode) at $targetPath"
        )
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
          dataset.rdd.getNumPartitions
        case fraction if fraction > 0.0 && fraction < 1.0 =>
          // Use sample to determine partitioning

          val minFraction =
            if (fraction * count >= 1) // Make sure we get at least on item in teh dataset
              fraction
            else if (count > 0) // We make sure we get at least 1 item which is 2 because of double imprecision for huge numbers.
              2 / count
            else
              0

          val sampledDataset = dataset.sample(false, minFraction)
          partitionedDatasetWriter(sampledDataset, metadata.getPartitionAttributes())
            .mode(SaveMode.ErrorIfExists)
            .format(settings.comet.writeFormat)
            .option("path", tmpPath.toString)
            .save()
          val consumed = storageHandler.spaceConsumed(tmpPath) / fraction
          val blocksize = storageHandler.blockSize(tmpPath)
          storageHandler.delete(tmpPath)
          Math.max(consumed / blocksize, 1).toInt
        case count if count >= 1.0 =>
          count.toInt
      }

      val partitionedDF =
        partitionedDatasetWriter(dataset.coalesce(nbPartitions), metadata.getPartitionAttributes())

      val mergePath = s"${targetPath.toString}.merge"
      val targetDataset = if (merge && area != StorageArea.rejected) {
        partitionedDF
          .mode(SaveMode.Overwrite)
          .format(settings.comet.writeFormat)
          .option("path", mergePath)
          .save()
        partitionedDatasetWriter(
          session.read.parquet(mergePath.toString),
          metadata.getPartitionAttributes()
        )
      } else
        partitionedDF
      val targetDatasetWriter = targetDataset
        .mode(saveMode)
        .format(settings.comet.writeFormat)
        .option("path", targetPath.toString)
      if (settings.comet.hive) {
        targetDatasetWriter.saveAsTable(fullTableName)
        val tableComment = schema.comment.getOrElse("")
        session.sql(s"ALTER TABLE $fullTableName SET TBLPROPERTIES ('comment' = '$tableComment')")
        if (settings.comet.analyze) {
          val allCols = session.table(fullTableName).columns.mkString(",")
          val analyzeTable =
            s"ANALYZE TABLE $fullTableName COMPUTE STATISTICS FOR COLUMNS $allCols"
          if (session.version.substring(0, 3).toDouble >= 2.4)
            try {
              session.sql(analyzeTable)
            } catch {
              case e: Throwable =>
                logger.warn(
                  s"Failed to compute statistics for table $fullTableName on columns $allCols"
                )
                e.printStackTrace()
            }
        }
      } else {
        targetDatasetWriter.save()
      }
      logger.info(s"Saved ${dataset.count()} rows to $targetPath")
      storageHandler.delete(new Path(mergePath))
      if (merge && area != StorageArea.rejected)
        logger.info(s"deleted merge file $mergePath")
      (targetDataset, mergePath)
    } else {
      logger.warn("Empty dataset with no columns won't be saved")
      (null, null)
    }
  }

  /**
    * Main entry point as required by the Spark Job interface
    *
    * @return : Spark Session used for the job
    */
  def run(): Try[SparkSession] = {
    domain.checkValidity() match {
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
              s"${settings.comet.jobId}",
              inputFiles,
              domain.name,
              schema.name,
              success = true,
              inputCount,
              acceptedCount,
              rejectedCount,
              start,
              end.getTime - start.getTime,
              "success"
            )
            SparkAuditLogWriter.append(session, log)
            schema.postsql.getOrElse(Nil).foreach(session.sql)
            Success(session)
          case Failure(exception) =>
            val end = Timestamp.from(Instant.now())
            val err = Utils.exceptionAsString(exception)
            AuditLog(
              s"${settings.comet.jobId}",
              path.map(_.toString).mkString(","),
              domain.name,
              schema.name,
              success = false,
              0,
              0,
              0,
              start,
              end.getTime - start.getTime,
              err
            )
            logger.error(err)
            Failure(throw exception)
        }
    }
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

  def saveRejected(
    session: SparkSession,
    rejectedRDD: RDD[String],
    domainName: String,
    schemaName: String,
    now: Timestamp
  )(implicit settings: Settings): Try[(Dataset[Row], Path)] = {
    import session.implicits._
    val rejectedPath = new Path(DatasetArea.rejected(domainName), schemaName)
    val rejectedPathName = rejectedPath.toString
    val jobid = s"${settings.comet.jobId}"
    val rejectedTypedRDD = rejectedRDD.map { err =>
      RejectedRecord(jobid, now, domainName, schemaName, err, rejectedPathName)
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

    val res = settings.comet.audit.index match {
      case IndexSinkSettings.BigQuery(dataset) =>
        val bqConfig = BigQueryLoadConfig(
          Right(rejectedDF),
          outputDataset = dataset,
          outputTable = "rejected",
          None,
          "parquet",
          "CREATE_IF_NEEDED",
          "WRITE_APPEND",
          None,
          None
        )
        new BigQueryLoadJob(bqConfig, Some(bigqueryRejectedSchema())).run()

      case IndexSinkSettings.Jdbc(jdbcName, partitions, batchSize) =>
        val jdbcConfig = JdbcLoadConfig.fromComet(
          jdbcName,
          settings.comet,
          Right(rejectedDF),
          "rejected",
          partitions = partitions,
          batchSize = batchSize
        )

        new JdbcLoadJob(jdbcConfig).run()

      case IndexSinkSettings.None =>
        Success(())
    }
    res match {
      case Success(_) => Success(rejectedDF, rejectedPath)
      case Failure(e) => Failure(e)
    }
  }

  def validateCol(
    colRawValue: String,
    colAttribute: Attribute,
    tpe: Type
  )(
    implicit /* TODO: make me explicit. Avoid rebuilding the PrivacyLevel(settings) at each invocation? */ settings: Settings
  ): ColResult = {
    def ltrim(s: String) = s.replaceAll("^\\s+", "")
    def rtrim(s: String) = s.replaceAll("\\s+$", "")
    val trimmedColValue = colAttribute.position.map { position =>
      position.trim match {
        case Some(LEFT)  => ltrim(colRawValue)
        case Some(RIGHT) => rtrim(colRawValue)
        case Some(BOTH)  => colRawValue.trim()
        case _           => colRawValue
      }
    } getOrElse (colRawValue)

    val colValue =
      if (trimmedColValue.length == 0) colAttribute.default.getOrElse("")
      else trimmedColValue

    val optionalColIsEmpty = !colAttribute.required && colValue.isEmpty
    val colPatternIsValid = tpe.matches(colValue)
    val privacyLevel = colAttribute.getPrivacy()
    val privacy =
      if (privacyLevel == PrivacyLevel.None)
        colValue
      else
        privacyLevel.encrypt(colValue)
    val colPatternOK = optionalColIsEmpty || colPatternIsValid
    val (sparkValue, colParseOK) =
      if (colPatternOK) {
        Try(tpe.sparkValue(privacy)) match {
          case Success(res) => (res, true)
          case Failure(_)   => (null, false)
        }
      } else
        (null, false)
    ColResult(
      ColInfo(
        colValue,
        colAttribute.name,
        tpe.name,
        tpe.pattern,
        colPatternOK && colParseOK
      ),
      sparkValue
    )
  }
}
