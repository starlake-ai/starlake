package com.ebiznext.comet.job

import java.sql.Timestamp
import java.time.Instant

import com.ebiznext.comet.config.{DatasetArea, HiveArea}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.schema.model.SchemaModel._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

case class ColInfo(colData: String, colName: String, typeName: String, pattern: String, success: Boolean)

case class RowInfo(timestamp: Timestamp, colInfos: List[ColInfo])

case class ColResult(colInfo: ColInfo, sparkValue: Any)

case class RowResult(colResults: List[ColResult]) {
  def isRejected: Boolean = colResults.exists(!_.colInfo.success)

  def isAccepted: Boolean = !isRejected
}


class DsvJob(domain: Domain, schema: SchemaModel.Schema, types: List[Type], metadata: Metadata, path: Path, storageHandler: StorageHandler) extends SparkJob {
  override def name: String = path.getName

  val schemaHeaders: List[String] = schema.attributes.map(_.name)

  def cleanHeaderCol(header: String): String = header.replaceAll("\"", "").replaceAll("\uFEFF", "")

  def validateHeader(datasetHeaders: List[String], schemaHeaders: List[String]): Boolean = {
    schemaHeaders.forall(schemaHeader => datasetHeaders.contains(schemaHeader))
  }

  def intersectHeaders(datasetHeaders: List[String], schemaHeaders: List[String]): (List[String], List[String]) = {
    datasetHeaders.partition(schemaHeaders.contains)
  }


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
    if (metadata.withHeader.getOrElse(false)) {
      val datasetHeaders: List[String] = df.columns.toList.map(cleanHeaderCol)
      val (_, drop) = intersectHeaders(datasetHeaders, schemaHeaders)
      if (drop.nonEmpty) {
        df.drop(drop: _*)
      }
      else
        df
    } else
      df
  }


  private val sparkType: StructType = {
    val mapTypes = types.map(tpe => tpe.name -> tpe).toMap
    val sparkFields = schema.attributes.map { attribute =>
      mapTypes(attribute.`type`).sparkType(attribute.name, !attribute.required)
    }
    StructType(sparkFields)
  }

  private val schemaTypes: List[Type] = {
    val mapTypes: Map[String, Type] = types.map(tpe => tpe.name -> tpe).toMap
    schema.attributes.map { attribute =>
      mapTypes(attribute.`type`)
    }
  }

  private def validate(dataset: DataFrame) = {
    val (rejectedRDD, acceptedRDD) = DsvIngestTask.validate(session, dataset, this.schema.attributes, schemaTypes, sparkType)

    val writeMode = metadata.getWrite()

    val rejectedPath = new Path(DatasetArea.path(domain.name, "rejected"), schema.name)
    saveRows(session.createDataFrame(rejectedRDD), rejectedPath, writeMode, HiveArea.rejected)

    val acceptedPath = new Path(DatasetArea.path(domain.name, "accepted"), schema.name)
    saveRows(session.createDataFrame(acceptedRDD, sparkType), acceptedPath, writeMode, HiveArea.accepted)
    //    saveRejectedRows(session.createDataFrame(rejectedRDD))
    //    saveAcceptedRows(session.createDataFrame(acceptedRDD, sparkType))
  }


  def saveRows(dataset: DataFrame, targetPath: Path, writeMode: Write, area: HiveArea): Unit = {
    val count = dataset.count()
    val saveMode = writeMode.toSaveMode
    val hiveDB = HiveArea.area(domain.name, area)
    val tableName = schema.name
    val fullTableName = s"$hiveDB.$tableName"
    logger.info(s"DSV Output $count to Hive table $hiveDB/$tableName($saveMode) at $targetPath")
    session.sql(s"create database if not exists $hiveDB")
    session.sql(s"use $hiveDB")
    session.sql(s"drop table if exists $hiveDB.$tableName")


    val partitionedDF = partitionedDatasetWriter(dataset, metadata.partition.getOrElse(Nil))

    partitionedDF.mode(saveMode).option("path", targetPath.toString).saveAsTable(fullTableName)

    val allCols = session.table(fullTableName).columns.mkString(",")
    val analyzeTable = s"ANALYZE TABLE $fullTableName COMPUTE STATISTICS FOR COLUMNS $allCols"
  }

  def run(args: Array[String]): SparkSession = {
    schema.presql.getOrElse(Nil).foreach(session.sql)
    validate(loadDataSet())
    schema.postsql.getOrElse(Nil).foreach(session.sql)
    session
  }
}

object DsvIngestTask {
  def validate(session: SparkSession, dataset: DataFrame, attributes: List[DSVAttribute], types: List[Type], sparkType: StructType): (RDD[RowInfo], RDD[Row]) = {
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
              // TODO if conditions below should be tested during schema load.
              case PrivacyLevel.HIDE if tpe.primitiveType == PrimitiveType.string =>
                ""
              case PrivacyLevel.MD5 if tpe.primitiveType == PrimitiveType.string =>
                Utils.md5(colValue)
              case PrivacyLevel.SHA1 if tpe.primitiveType == PrimitiveType.string =>
                Utils.sha1(colValue)
              case PrivacyLevel.SHA256 if tpe.primitiveType == PrimitiveType.string =>
                Utils.sha256(colValue)
              case PrivacyLevel.SHA512 if tpe.primitiveType == PrimitiveType.string =>
                Utils.sha512(colValue)
              case PrivacyLevel.SHA512 if tpe.primitiveType == PrimitiveType.string =>
                // TODO Implement AES
                throw new Exception("AES Not yet implemented")
              case _ =>
                // TODO should never happen  if schema is checked at load time
                colValue
            }
            val success = validNumberOfColumns && (optionalColIsEmpty || colPatternIsValid)
            val sparkValue = if (success)
              tpe.primitiveType.fromString(privacy)
            else
              null
            ColResult(ColInfo(colValue, colAttribute.name, tpe.name, tpe.pattern.pattern(), success), sparkValue)
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

