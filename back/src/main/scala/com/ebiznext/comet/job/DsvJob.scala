package com.ebiznext.comet.job

import java.sql.Timestamp
import java.time.Instant

import com.ebiznext.comet.config.{DatasetArea, HiveArea, Settings}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.Rejection.{ColInfo, ColResult, RowInfo, RowResult}
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}


/**
  * Main class to ingest delimiter separated values file
  *
  * @param domain         : Input Dataset Domain
  * @param schema         : Input Dataset Schema
  * @param types          : List of globally defined types
  * @param path           : Input dataset path
  * @param storageHandler : Storage Handler
  */
class DsvJob(domain: Domain, schema: Schema, types: List[Type], path: Path, storageHandler: StorageHandler) extends SparkJob {
  /**
    *
    * @return Spark Job name
    */
  override def name: String = path.getName

  /**
    * Merged metadata
    */
  val metadata = domain.metadata.getOrElse(Metadata()).`import`(schema.metadata.getOrElse(Metadata()))

  /**
    * dataset Header names as defined by the schema
    */
  val schemaHeaders: List[String] = schema.attributes.map(_.name)

  /**
    * remove any extra quote / BOM in the header
    *
    * @param header : Header column name
    * @return
    */
  def cleanHeaderCol(header: String): String = header.replaceAll("\"", "").replaceAll("\uFEFF", "")

  /**
    *
    * @param datasetHeaders : Headers found in the dataset
    * @param schemaHeaders  : Headers defined in the schema
    * @return success  if all headers in the schema exist in the dataset
    */
  def validateHeader(datasetHeaders: List[String], schemaHeaders: List[String]): Boolean = {
    schemaHeaders.forall(schemaHeader => datasetHeaders.contains(schemaHeader))
  }

  /**
    *
    * @param datasetHeaders : Headers found in the dataset
    * @param schemaHeaders  : Headers defined in the schema
    * @return two lists : One with thecolumns present in the schema and the dataset and onther with the headers present in the dataset only
    */
  def intersectHeaders(datasetHeaders: List[String], schemaHeaders: List[String]): (List[String], List[String]) = {
    datasetHeaders.partition(schemaHeaders.contains)
  }


  /**
    * Load dataset using spark csv reader and all metadata. Does not infer schema.
    * columns not defined in the schema are dropped fro the dataset (require datsets with a header)
    *
    * @return Spark Dataset
    */
  def loadDataSet(): DataFrame = {
    val df = session.read.format("com.databricks.spark.csv")
      .option("header", metadata.isWithHeader().toString)
      .option("inferSchema", value = false)
      .option("delimiter", metadata.getSeparator())
      .option("quote", metadata.getQuote())
      .option("escape", metadata.getEscape())
      .option("parserLib", "UNIVOCITY")
      .csv(path.toString)
    df.show()
    metadata.withHeader match {
      case Some(true) =>
        val datasetHeaders: List[String] = df.columns.toList.map(cleanHeaderCol)
        val (_, drop) = intersectHeaders(datasetHeaders, schemaHeaders)
        if (datasetHeaders.length == drop.length) {
          throw new Exception(
            s"""No attribute found in input dataset ${path.toString}
               | SchemaHeaders : ${schemaHeaders.mkString(",")}
               | Dataset Headers : ${datasetHeaders.mkString(",")}
             """.stripMargin)
        }
        df.drop(drop: _*)
      case Some(false) | None =>
        df
    }
  }

  /**
    * Return Schema as a spark StructType
    *
    */
  private val sparkType: StructType = {
    val mapTypes = types.map(tpe => tpe.name -> tpe).toMap
    val sparkFields = schema.attributes.map { attribute =>
      mapTypes(attribute.`type`).sparkType(attribute.name, !attribute.required, attribute.comment)
    }
    StructType(sparkFields)
  }

  /**
    * Return the ordered list of types used in the schema
    */
  private val schemaTypes: List[Type] = {
    val mapTypes: Map[String, Type] = types.map(tpe => tpe.name -> tpe).toMap
    schema.attributes.map { attribute =>
      mapTypes(attribute.`type`)
    }
  }

  /**
    * Apply the schema to the dataset. This is where all the magic happen
    * Valid records are stored in the accepted path / table and invalid records in the rejected path / table
    *
    * @param dataset : Spark Dataset
    */
  private def validate(dataset: DataFrame)

  = {
    val (rejectedRDD, acceptedRDD) = DsvIngestTask.validate(session, dataset, this.schema.attributes, metadata.getDateFormat(), metadata.getTimestampFormat(), schemaTypes, sparkType)
    logger.whenInfoEnabled {
      val inputCount = dataset.count()
      val acceptedCount = acceptedRDD.count()
      val rejectedCount = rejectedRDD.count()
      val inputFiles = dataset.inputFiles.mkString(",")
      logger.info(s"ingestion-summary -> files: [$inputFiles], input: $inputCount, accepted: $acceptedCount, rejected:$rejectedCount")
    }
    val writeMode = metadata.getWrite()

    val rejectedPath = new Path(DatasetArea.rejected(domain.name), schema.name)
    saveRows(session.createDataFrame(rejectedRDD), rejectedPath, writeMode, HiveArea.rejected)

    val acceptedPath = new Path(DatasetArea.accepted(domain.name), schema.name)
    val renamedAttributes = schema.renamedAttributes().toMap
    logger.whenInfoEnabled {
      renamedAttributes.foreach { case (name, rename) =>
        logger.info(s"renaming column $name to $rename")
      }
    }
    val acceptedDF = session.createDataFrame(acceptedRDD, sparkType)
    val cols = acceptedDF.columns.map { column =>
      org.apache.spark.sql.functions.col(column).as(renamedAttributes.getOrElse(column, column))
    }
    saveRows(acceptedDF.select(cols: _*), acceptedPath, writeMode, HiveArea.accepted)
  }


  /**
    * Save typed dataset in parquet. If hive support is active, also register it as a Hive Table and if analyze is active, also compute basic statistics
    *
    * @param dataset    : dataset to save
    * @param targetPath : absolute path
    * @param writeMode  : Append or overwrite
    * @param area       : accpeted or rejected area
    */
  def saveRows(dataset: DataFrame, targetPath: Path, writeMode: Write, area: HiveArea): Unit = {
    val count = dataset.count()
    val saveMode = writeMode.toSaveMode
    val hiveDB = HiveArea.area(domain.name, area)
    val tableName = schema.name
    val fullTableName = s"$hiveDB.$tableName"
    if (Settings.comet.hive) {
      logger.info(s"DSV Output $count to Hive table $hiveDB/$tableName($saveMode) at $targetPath")
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
  }


  /**
    * Main entry point as required by the Spark Job interface
    *
    * @param args : arbitrary list of arguments
    * @return : Spark Session used for the job
    */
  def run(args: Array[String]): SparkSession = {
    schema.presql.getOrElse(Nil).foreach(session.sql)
    validate(loadDataSet())
    schema.postsql.getOrElse(Nil).foreach(session.sql)
    session
  }
}


/**
  * The Spark task that run on each worker
  */
object DsvIngestTask {
  /**
    * For each col of each row
    *   - we extract the col value / the col constraints / col type
    *   - we check that the constraints are verified
    *   - we apply any required privacy transformation
    *   - parse the column into the target primitive Spark Type
    * We end up using catalyst to create a Spark Row
    *
    * @param session    : The Spark session
    * @param dataset    : The dataset
    * @param attributes : the col attributes
    * @param dateFormat : expected java date pattern in the dataset
    * @param timeFormat : expected java timestamp pattern in the dataset
    * @param types      : List of globally defined types
    * @param sparkType  : The expected Spark Type for valid rows
    * @return Two RDDs : One RDD for rejected rows and one RDD for accepted rows
    */
  def validate(session: SparkSession, dataset: DataFrame, attributes: List[Attribute], dateFormat: String, timeFormat: String, types: List[Type], sparkType: StructType): (RDD[RowInfo], RDD[Row]) = {
    val now = Timestamp.from(Instant.now)
    val rdds = dataset.rdd
    dataset.show()
    val checkedRDD: RDD[RowResult] = dataset.rdd.mapPartitions { partition =>
      partition.map { row: Row =>
        val rowCols = row.toSeq.zip(attributes).map {
          case (colValue, colAttribute) => (Option(colValue).getOrElse("").toString, colAttribute)
        }.zip(types)
        RowResult(
          rowCols.map { case ((colValue, colAttribute), tpe) =>
            val validNumberOfColumns = attributes.length <= rowCols.length
            val optionalColIsEmpty = !colAttribute.required && colValue.isEmpty
            val colPatternIsValid = tpe.pattern.matcher(colValue).matches()
            val privacy = colAttribute.privacy match {
              case PrivacyLevel.NONE =>
                colValue
              case PrivacyLevel.HIDE =>
                ""
              case PrivacyLevel.MD5 =>
                Encryption.md5(colValue)
              case PrivacyLevel.SHA1 =>
                Encryption.sha1(colValue)
              case PrivacyLevel.SHA256 =>
                Encryption.sha256(colValue)
              case PrivacyLevel.SHA512 =>
                Encryption.sha512(colValue)
              case PrivacyLevel.AES =>
                // TODO Implement AES
                throw new Exception("AES Not yet implemented")
              case _ =>
                // should never happen
                colValue
            }
            val colPatternOK = validNumberOfColumns && (optionalColIsEmpty || colPatternIsValid)
            val (sparkValue, colParseOK) =
              if (colPatternOK) {
                Try(tpe.primitiveType.fromString(privacy, dateFormat, timeFormat)) match {
                  case Success(res) => (res, true)
                  case Failure(_) => (null, false)
                }
              }
              else
                (null, false)
            ColResult(ColInfo(colValue, colAttribute.name, tpe.name, tpe.pattern.pattern(), colPatternOK && colParseOK), sparkValue)
          } toList
        )
      }
    } cache()

    val rejectedRDD: RDD[RowInfo] = checkedRDD.filter(_.isRejected).map(rr => RowInfo(now, rr.colResults.map(_.colInfo)))

    val acceptedRDD: RDD[Row] = checkedRDD.filter(_.isAccepted).map { rowResult =>
      val sparkValues: List[Any] = rowResult.colResults.map(_.sparkValue)
      new GenericRowWithSchema(Row(sparkValues: _*).toSeq.toArray, sparkType)
    }
    (rejectedRDD, acceptedRDD)
  }
}

