package com.ebiznext.comet.job

import com.ebiznext.comet.config.{DatasetArea, HiveArea, Settings}
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
  *
  */
trait IngestionJob extends SparkJob {
  def domain: Domain

  def schema: Schema

  def types: List[Type]

  /**
    * Merged metadata
    */
  lazy val metadata: Metadata = domain.metadata.getOrElse(Metadata()).`import`(schema.metadata.getOrElse(Metadata()))

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
  def ingest(dataset: DataFrame): Unit

  def saveRejected(rejectedRDD: RDD[String]) = {
    val writeMode = metadata.getWrite()
    val rejectedPath = new Path(DatasetArea.rejected(domain.name), schema.name)
    import session.implicits._
    saveRows(rejectedRDD.toDF, rejectedPath, writeMode, HiveArea.rejected)
  }


  def saveAccepted(acceptedRDD: RDD[Row]): Unit

  /**
    * Save typed dataset in parquet. If hive support is active, also register it as a Hive Table and if analyze is active, also compute basic statistics
    *
    * @param dataset    : dataset to save
    * @param targetPath : absolute path
    * @param writeMode  : Append or overwrite
    * @param area       : accepted or rejected area
    */
  def saveRows(dataset: DataFrame, targetPath: Path, writeMode: Write, area: HiveArea): Unit = {
    if (dataset.columns.size > 0) {
      val count = dataset.count()
      val saveMode = writeMode.toSaveMode
      val hiveDB = HiveArea.area(domain.name, area)
      val tableName = schema.name
      val fullTableName = s"$hiveDB.$tableName"
      if (Settings.comet.hive) {
        logger.info(s"DSV Output $count records to Hive table $hiveDB/$tableName($saveMode) at $targetPath")
        val dbComment = domain.comment.getOrElse("")
        session.sql(s"create database if not exists $hiveDB comment '$dbComment'")
        session.sql(s"use $hiveDB")
        session.sql(s"drop table if exists $hiveDB.$tableName")
      }

      val partitionedDF = partitionedDatasetWriter(dataset, metadata.partition.getOrElse(Nil))

      val targetDataset = partitionedDF.mode(saveMode).format("parquet").option("path", targetPath.toString)
      if (Settings.comet.hive) {
        targetDataset.saveAsTable(fullTableName)
        val tableComment = schema.comment.getOrElse("")
        session.sql(s"ALTER TABLE $fullTableName SET TBLPROPERTIES ('comment' = '$tableComment')")
        if (Settings.comet.analyze) {
          val allCols = session.table(fullTableName).columns.mkString(",")
          val analyzeTable = s"ANALYZE TABLE $fullTableName COMPUTE STATISTICS FOR COLUMNS $allCols"
          if (session.version.substring(0, 3).toDouble >= 2.4)
            session.sql(analyzeTable)
        }
      }
      else {
        targetDataset.save()
      }
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
  def run(args: Array[String]): SparkSession = {
    domain.checkValidity(types) match {
      case Left(errors) =>
        errors.foreach(println)
      case Right(true) =>
        schema.presql.getOrElse(Nil).foreach(session.sql)
        ingest(loadDataSet())
        schema.postsql.getOrElse(Nil).foreach(session.sql)
    }
    session
  }

}
