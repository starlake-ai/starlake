package com.ebiznext.comet.job

import java.sql.Timestamp
import java.time.Instant

import com.ebiznext.comet.config.Area
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.schema.model.SchemaModel.Write.{APPEND, OVERWRITE}
import com.ebiznext.comet.schema.model.SchemaModel.{Domain, Metadata, Type}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

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


  case class ColResult(colData: String, colName: String, typeName: String, pattern: String, success: Boolean, sparkValue: Any)


  case class RowResult(colResults: List[ColResult]) {
    def isRejected: Boolean = colResults.exists(!_.success)

    def isAccepted: Boolean = !isRejected
  }

  private def sparkType() = {
    val mapTypes = types.map(tpe => tpe.name -> tpe).toMap
    val sparkFields = schema.attributes.map { attribute =>
      mapTypes(attribute.`type`).sparkType(!attribute.required)
    }
    StructType(sparkFields)
  }

  private def validate(dataset: DataFrame) = {
    val now = Timestamp.from(Instant.now)
    val checkedRDD: RDD[RowResult] = dataset.rdd.mapPartitions { partition =>
      partition.map { row: Row =>
        val rowCols = row.toSeq.zip(schemaHeaders).map {
          case (colValue, colName) => (Option(colValue).getOrElse("").toString, colName)
        }.zip(types)
        RowResult(
          rowCols.map { case ((colValue, colName), tpe) =>
            val sparkValue = if (tpe.pattern.matcher(colValue).matches())
              tpe.primitiveType.fromString(colValue)
            else
              null
            ColResult(colValue, colName, tpe.name, tpe.pattern.pattern(), true, sparkValue)
          } toList
        )
      }
    } cache()

    val rejectedRDD: RDD[RowResult] = checkedRDD.filter(_.isRejected)
    saveRejectedRows(session.createDataFrame(rejectedRDD))

    val acceptedRDD: RDD[Row] = checkedRDD.filter(_.isAccepted).map { rowResult =>
      val sparkValues: List[Any] = rowResult.colResults.map(_.sparkValue)
      new GenericRowWithSchema(Row(sparkValues).toSeq.toArray, sparkType())
    }

    saveAcceptedRows(session.createDataFrame(acceptedRDD, sparkType()))
  }

  def saveAcceptedRows(dataset: DataFrame): Unit = {
    val validPath = Area.path(Area.accepted(domain.name), schema.name)
    val count = dataset.count()
    val saveMode = metadata.getWrite() match {
      case OVERWRITE => SaveMode.Overwrite
      case APPEND => SaveMode.Append
    }
    logger.info(s"Output $count to $validPath in $saveMode")
    dataset.write.mode(saveMode).parquet(validPath.toString)
  }

  def saveRejectedRows(dataset: DataFrame): Unit = {
    val path = Area.path(Area.rejected(domain.name), schema.name)
    val count = dataset.count()
    val saveMode = metadata.getWrite() match {
      case OVERWRITE => SaveMode.Overwrite
      case APPEND => SaveMode.Append
    }
    logger.info(s"Output $count to $path in $saveMode")
    dataset.write.mode(saveMode).parquet(path.toString)
  }

  def run(args: Array[String]): Unit = {
    validate(loadDataSet())
    val targetPath = new Path(Area.staging(domain.name), path.getName)
    storageHandler.move(path, targetPath)
  }
}
