package com.ebiznext.comet.job

import java.sql.Timestamp
import java.time.Instant

import com.ebiznext.comet.config.{DatasetArea, HiveArea}
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
class DsvIngestionJob(val domain: Domain, val schema: Schema, val types: List[Type], val path: Path, storageHandler: StorageHandler) extends IngestionJob {
  /**
    *
    * @return Spark Job name
    */
  override def name: String = path.getName


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
    df.printSchema()
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
  def ingest(dataset: DataFrame): Unit = {
    val (rejectedRDD, acceptedRDD) = DsvIngestionUtil.validate(session, dataset, this.schema.attributes, metadata.getDateFormat(), metadata.getTimestampFormat(), schemaTypes, sparkType)
    logger.whenInfoEnabled {
      val inputCount = dataset.count()
      val acceptedCount = acceptedRDD.count()
      val rejectedCount = rejectedRDD.count()
      val inputFiles = dataset.inputFiles.mkString(",")
      logger.info(s"ingestion-summary -> files: [$inputFiles], input: $inputCount, accepted: $acceptedCount, rejected:$rejectedCount")
    }
    saveRejected(rejectedRDD)
    saveAccepted(acceptedRDD)
  }

  def saveAccepted(acceptedRDD: RDD[Row]) = {
    val writeMode = metadata.getWrite()
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

}


/**
  * The Spark task that run on each worker
  */
object DsvIngestionUtil {
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
  def validate(session: SparkSession, dataset: DataFrame, attributes: List[Attribute], dateFormat: String, timeFormat: String, types: List[Type], sparkType: StructType): (RDD[String], RDD[Row]) = {
    val now = Timestamp.from(Instant.now)
    val rdds = dataset.rdd
    dataset.printSchema()
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

    val rejectedRDD: RDD[String] = checkedRDD.filter(_.isRejected).map(rr => RowInfo(now, rr.colResults.map(_.colInfo)).toString)

    val acceptedRDD: RDD[Row] = checkedRDD.filter(_.isAccepted).map { rowResult =>
      val sparkValues: List[Any] = rowResult.colResults.map(_.sparkValue)
      new GenericRowWithSchema(Row(sparkValues: _*).toSeq.toArray, sparkType)
    }
    (rejectedRDD, acceptedRDD)
  }
}

