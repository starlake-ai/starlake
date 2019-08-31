package com.ebiznext.comet.job.ingest

import com.ebiznext.comet.config.{DatasetArea, HiveArea, Settings}
import com.ebiznext.comet.job.metrics.MetricsJob
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.Rejection.{ColInfo, ColResult}
import com.ebiznext.comet.schema.model.Trim.{BOTH, LEFT, RIGHT}
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.{SparkJob, Utils}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

/**
  *
  */
trait IngestionJob extends SparkJob {
  def domain: Domain

  def schema: Schema

  def storageHandler: StorageHandler

  def types: List[Type]

  /**
    * Merged metadata
    */
  lazy val metadata: Metadata = schema.mergedMetadata(domain.metadata)

  /**
    * Dataset loading strategy (JSOn / CSV / ...)
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

  def saveRejected(rejectedRDD: RDD[String]): Unit = {
    val writeMode = metadata.getWriteMode()
    val rejectedPath = new Path(DatasetArea.rejected(domain.name), schema.name)
    import session.implicits._
    rejectedRDD.toDF.show(100, false)
    saveRows(rejectedRDD.toDF, rejectedPath, writeMode, HiveArea.rejected, false)
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
  def saveAccepted(acceptedDF: DataFrame): Unit = {
    if (Settings.comet.metrics.active) {
      new MetricsJob(this.domain, this.schema, Stage.UNIT, this.storageHandler)
        .run(acceptedDF, System.currentTimeMillis())
    }
    val writeMode = getWriteMode()
    val acceptedPath = new Path(DatasetArea.accepted(domain.name), schema.name)
    val mergedDF = schema.merge.map { mergeOptions =>
      if (storageHandler.exist(new Path(acceptedPath, "_SUCCESS"))) {
        val existingDF = session.read.parquet(acceptedPath.toString)
        merge(acceptedDF, existingDF, mergeOptions)
      } else
        acceptedDF
    } getOrElse (acceptedDF)

    saveRows(mergedDF, acceptedPath, writeMode, HiveArea.accepted, schema.merge.isDefined)
    if (Settings.comet.metrics.active) {
      new MetricsJob(this.domain, this.schema, Stage.GLOBAL, storageHandler).run()
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

    // Force orderinfg of columns to be the same
    val orderedExisting = existingDF.select(partitionedInputDF.columns.map((col(_))): _*)

    // Force orderinfg again of columns to be the same since join operation change it otherwise except below won"'t work.
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
    if (Settings.comet.mergeForceDistinct)
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
    area: HiveArea,
    merge: Boolean
  ): Unit = {
    if (dataset.columns.length > 0) {
      val count = dataset.count()
      val saveMode = writeMode.toSaveMode
      val hiveDB = HiveArea.area(domain.name, area)
      val tableName = schema.name
      val fullTableName = s"$hiveDB.$tableName"
      if (Settings.comet.hive) {
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
            .format(Settings.comet.writeFormat)
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
      val targetDataset = if (merge) {
        partitionedDF
          .mode(SaveMode.Overwrite)
          .format(Settings.comet.writeFormat)
          .option("path", mergePath)
          .save()
        partitionedDatasetWriter(
          session.read.parquet(mergePath.toString),
          metadata.getPartitionAttributes()
        )
      } else
        partitionedDF
      val finalDataset = targetDataset
        .mode(saveMode)
        .format(Settings.comet.writeFormat)
        .option("path", targetPath.toString)
      if (Settings.comet.hive) {
        finalDataset.saveAsTable(fullTableName)
        val tableComment = schema.comment.getOrElse("")
        session.sql(s"ALTER TABLE $fullTableName SET TBLPROPERTIES ('comment' = '$tableComment')")
        if (Settings.comet.analyze) {
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
        finalDataset.save()
      }
      val _ = storageHandler.delete(new Path(mergePath))

    } else {
      logger.warn("Empty dataset with no columns won't be saved")
    }
  }

  /**
    * Main entry point as required by the Spark Job interface
    *
    * @return : Spark Session used for the job
    */
  def run(): SparkSession = {
    domain.checkValidity(types) match {
      case Left(errors) =>
        errors.foreach(err => logger.error(err))
      case Right(_) =>
        val start = System.currentTimeMillis()
        schema.presql.getOrElse(Nil).foreach(session.sql)
        val dataset = loadDataSet()
        dataset match {
          case Success(dataset) =>
            val (rejectedRDD, acceptedRDD) = ingest(dataset)
            logger.whenInfoEnabled {
              val inputCount = dataset.count()
              val acceptedCount = acceptedRDD.count()
              val rejectedCount = rejectedRDD.count()
              val inputFiles = dataset.inputFiles.mkString(",")
              logger.info(
                s"ingestion-summary -> files: [$inputFiles], domain: ${domain.name}, schema: ${schema.name}, input: $inputCount, accepted: $acceptedCount, rejected:$rejectedCount"
              )
              val end = System.currentTimeMillis()
              IngestionLog(
                inputFiles,
                domain.name,
                schema.name,
                success = true,
                inputCount,
                acceptedCount,
                rejectedCount,
                start,
                end - start
              )
            }
            schema.postsql.getOrElse(Nil).foreach(session.sql)
          case Failure(exception) =>
            Utils.logException(logger, exception)
            throw exception
        }
    }
    session
  }

}

object IngestionUtil {

  def validateCol(
    validNumberOfColumns: Boolean,
    colRawValue: String,
    colAttribute: Attribute,
    tpe: Type
  ) = {
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
    val colPatternOK = validNumberOfColumns && (optionalColIsEmpty || colPatternIsValid)
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
