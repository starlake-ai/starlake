package com.ebiznext.comet.job

import java.sql.Timestamp
import java.time.Instant

import com.ebiznext.comet.config.{DatasetArea, HiveArea}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.schema.model.SchemaModel.Write.{APPEND, OVERWRITE}
import com.ebiznext.comet.schema.model.SchemaModel.{Domain, Metadata, Type, Write}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

case class ColInfo(colData: String, colName: String, typeName: String, pattern: String)

case class RowInfo(colInfos: List[ColInfo])

case class ColResult(colInfo: ColInfo, success: Boolean, sparkValue: Any)

case class RowResult(colResults: List[ColResult]) {
  def isRejected: Boolean = colResults.exists(!_.success)

  def isAccepted: Boolean = !isRejected
}


class DsvJob(domain: Domain, schema: SchemaModel.Schema, types: List[Type], metadata: Metadata, path: Path, storageHandler: StorageHandler) extends SparkJob {
  override def name: String = path.getName

  val schemaHeaders: List[String] = schema.attributes.map(_.name)

  def cleanHeaderCol(header: String): String = header.toLowerCase.replaceAll("\"", "").replaceAll("\uFEFF", "")

  def validateHeader(datasetHeaders: List[String], schemaHeaders: List[String]): Boolean = {
    schemaHeaders.forall(schemaHeader => datasetHeaders.contains(schemaHeader))
  }

  def intersectHeaders(datasetHeaders: List[String], schemaHeaders: List[String]): (List[String], List[String]) = {
    datasetHeaders.partition(schemaHeaders.contains)
  }


  private def loadDataSet(): DataFrame = {
    val df = session.read.format("com.databricks.spark.csv")
      .option("header", metadata.isWithHeader().toString)
      .option("inferSchema", value = false)
      .option("delimiter", metadata.getSeparator())
      .option("quote", metadata.getQuote())
      .option("escape", metadata.getEscape())
      .option("parserLib", "UNIVOCITY")
      .csv(path.toString)
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

  private def validate(dataset: DataFrame) = {
    val (rejectedRDD, acceptedRDD) = DsvTask.validate(session, dataset, this.schemaHeaders, types, sparkType)
    val writeMode = metadata.getWrite()

    val rejectedPath = DatasetArea.path(domain.name, "rejected")
    savedRows(session.createDataFrame(rejectedRDD), rejectedPath, writeMode,HiveArea.rejected)

    val acceptedPath = DatasetArea.path(domain.name, "accepted")
    savedRows(session.createDataFrame(acceptedRDD, sparkType), acceptedPath, writeMode,HiveArea.accepted)
//    saveRejectedRows(session.createDataFrame(rejectedRDD))
//    saveAcceptedRows(session.createDataFrame(acceptedRDD, sparkType))
  }

  def saveAcceptedRows(dataset: DataFrame): Unit = {
    val count = dataset.count()
    val validPath = DatasetArea.path(DatasetArea.accepted(domain.name), schema.name)
    val saveMode = metadata.getWrite() match {
      case OVERWRITE => SaveMode.Overwrite
      case APPEND => SaveMode.Append
    }
    val acceptedHiveDB = HiveArea.accepted(domain.name)
    val tableName = schema.name

    logger.info(s"Output $count to $validPath in $saveMode")
    session.sql(s"create database if not exists $acceptedHiveDB")
    session.sql(s"use $acceptedHiveDB")
    session.sql(s"drop table if exists $tableName")
    dataset.write.mode(saveMode).option("path", validPath.toString).saveAsTable(schema.name)
  }

  def saveRejectedRows(dataset: DataFrame): Unit = {
    val count = dataset.count()
    val rejectedtPath = DatasetArea.path(DatasetArea.rejected(domain.name), schema.name)
    val saveMode = metadata.getWrite() match {
      case OVERWRITE => SaveMode.Overwrite
      case APPEND => SaveMode.Append
    }
    val rejectedHiveDB = HiveArea.rejected(domain.name)
    val tableName = schema.name

    logger.info(s"Output $count to $rejectedtPath in $saveMode")
    session.sql(s"create database if not exists $rejectedHiveDB")
    session.sql(s"use $rejectedHiveDB")
    session.sql(s"drop table if exists $tableName")
    dataset.write.mode(saveMode).option("path", rejectedtPath.toString).saveAsTable(tableName)
  }

  def savedRows(dataset: DataFrame, targetPath: Path, writeMode: Write, area: HiveArea): Unit = {
    val count = dataset.count()
    val saveMode = writeMode match {
      case OVERWRITE => SaveMode.Overwrite
      case APPEND => SaveMode.Append
    }
    val hiveDB = HiveArea.area(domain.name, area)
    val tableName = schema.name

    logger.info(s"Output $count to $targetPath in $saveMode")
    session.sql(s"create database if not exists $hiveDB")
    session.sql(s"use $hiveDB")
    session.sql(s"drop table if exists $tableName")
    dataset.write.mode(saveMode).option("path", targetPath.toString).saveAsTable(tableName)
  }

  def run(args: Array[String]): Unit = {
    validate(loadDataSet())
    val targetPath = new Path(DatasetArea.staging(domain.name), path.getName)
    storageHandler.move(path, targetPath)
  }
}

object DsvTask {
  def validate(session: SparkSession, dataset: DataFrame, schemaHeaders: List[String], types: List[Type], sparkType: StructType): (RDD[RowInfo], RDD[Row]) = {
    val now = Timestamp.from(Instant.now)
    val checkedRDD: RDD[RowResult] = dataset.rdd.mapPartitions { partition =>
      partition.map { row: Row =>
        val rowCols = row.toSeq.zip(schemaHeaders).map {
          case (colValue, colName) => (Option(colValue).getOrElse("").toString, colName)
        }.zip(types)
        RowResult(
          rowCols.map { case ((colValue, colName), tpe) =>
            val success = tpe.pattern.matcher(colValue).matches()
            val sparkValue = if (success)
              tpe.primitiveType.fromString(colValue)
            else
              null
            ColResult(ColInfo(colValue, colName, tpe.name, tpe.pattern.pattern()), success, sparkValue)
          } toList
        )
      }
    } cache()

    val rejectedRDD: RDD[RowInfo] = checkedRDD.filter(_.isRejected).map(rr => RowInfo(rr.colResults.map(_.colInfo)))

    val acceptedRDD: RDD[Row] = checkedRDD.filter(_.isAccepted).map { rowResult =>
      val sparkValues: List[Any] = rowResult.colResults.map(_.sparkValue)
      new GenericRowWithSchema(Row(sparkValues).toSeq.toArray, sparkType)
    }
    (rejectedRDD, acceptedRDD)
  }
}