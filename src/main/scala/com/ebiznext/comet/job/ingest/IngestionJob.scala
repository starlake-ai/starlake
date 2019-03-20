package com.ebiznext.comet.job.ingest

import com.ebiznext.comet.config.{DatasetArea, HiveArea, Settings}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model._
import com.ebiznext.comet.utils.SparkJob
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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
  def loadDataSet(): DataFrame

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
    session.sparkContext.getRDDStorageInfo
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
    val toDeleteDF = existingDF.join(inputDF.select(merge.key.head, merge.key.tail: _*), merge.key)
    val updatesDF = merge.delete
      .map(condition => inputDF.filter(s"not ($condition)"))
      .getOrElse(inputDF)
    logger.whenDebugEnabled {
      logger.debug(s"Merge detected ${toDeleteDF.count()} items to update/delete")
      logger.debug(s"Merge detected ${updatesDF.count()} items to update/insert")
    }
    existingDF.except(toDeleteDF).union(updatesDF)
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
        session.sql(s"drop table if exists $hiveDB.$tableName")
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
    * @param args : arbitrary list of arguments
    * @return : Spark Session used for the job
    */
  def run(): SparkSession = {
    domain.checkValidity(types) match {
      case Left(errors) =>
        errors.foreach(err => logger.error(err))
      case Right(_) =>
        schema.presql.getOrElse(Nil).foreach(session.sql)
        val dataset = loadDataSet()
        val (rejectedRDD, acceptedRDD) = ingest(dataset)
        logger.whenInfoEnabled {
          val inputCount = dataset.count()
          val acceptedCount = acceptedRDD.count()
          val rejectedCount = rejectedRDD.count()
          val inputFiles = dataset.inputFiles.mkString(",")
          logger.info(
            s"ingestion-summary -> files: [$inputFiles], input: $inputCount, accepted: $acceptedCount, rejected:$rejectedCount"
          )
        }

        schema.postsql.getOrElse(Nil).foreach(session.sql)
    }
    session
  }

}
