package com.ebiznext.comet.job.validator

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.job.ingest.IngestionUtil
import com.ebiznext.comet.schema.model.Rejection.{ColInfo, ColResult, RowInfo, RowResult}
import com.ebiznext.comet.schema.model.{Attribute, Format, Type}
import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.time.Instant

object FlatRowValidator extends GenericRowValidator {

  private def toOriginalFormat(row: Row, format: Format, separator: String): String = {
    format match {
      case Format.DSV =>
        row.toSeq.map(Option(_).getOrElse("").toString).mkString(separator)
      case Format.SIMPLE_JSON =>
        val rowAsMap = row.getValuesMap(row.schema.fieldNames)
        new Gson().toJson(rowAsMap)
      case Format.POSITION =>
        row.toSeq.map(_.toString).mkString("")
      case _ =>
        throw new Exception("Should never happen")

    }
  }

  override def validate(
    session: SparkSession,
    format: Format,
    separator: String,
    dataset: DataFrame,
    attributes: List[Attribute],
    types: List[Type],
    sparkType: StructType
  )(implicit settings: Settings): ValidationResult = {
    val now = Timestamp.from(Instant.now)
    val checkedRDD: RDD[RowResult] = dataset.rdd
      .mapPartitions { partition =>
        partition.map { row =>
          val rowValues: Seq[(Option[String], Attribute)] = row.toSeq
            .zip(attributes)
            .map { case (colValue, colAttribute) =>
              (Option(colValue).map(_.toString), colAttribute)
            }
          val rowCols = rowValues.zip(types)
          lazy val colMap = rowValues.map { case (colValue, colAttr) =>
            (colAttr.name, colValue)
          }.toMap
          val validNumberOfColumns = attributes.length <= rowCols.length
          if (!validNumberOfColumns) {
            RowResult(
              rowCols.map { case ((colRawValue, colAttribute), tpe) =>
                ColResult(
                  ColInfo(
                    colRawValue,
                    colAttribute.name,
                    tpe.name,
                    tpe.pattern,
                    success = false
                  ),
                  null
                )
              }.toList,
              row.getAs[String](Settings.cometInputFileNameColumn),
              toOriginalFormat(row, format, separator)
            )
          } else {
            RowResult(
              rowCols.map { case ((colRawValue, colAttribute), tpe) =>
                IngestionUtil.validateCol(colRawValue, colAttribute, tpe, colMap)
              }.toList,
              row.getAs[String](Settings.cometInputFileNameColumn),
              toOriginalFormat(row, format, separator)
            )
          }
        }
      } persist (settings.comet.cacheStorageLevel)

    val rejectedRDD: RDD[String] = checkedRDD
      .filter(_.isRejected)
      .map(rowResult =>
        RowInfo(
          now,
          rowResult.colResults.filter(!_.colInfo.success).map(_.colInfo),
          rowResult.inputFileName
        ).toString
      )

    val rejectedInputLinesRDD: RDD[String] = checkedRDD.filter(_.isRejected).map(_.inputLine)

    val acceptedRDD: RDD[Row] = checkedRDD.filter(_.isAccepted).map { rowResult =>
      val sparkValues: List[Any] = rowResult.colResults.map(_.sparkValue)
      new GenericRowWithSchema(Row(sparkValues: _*).toSeq.toArray, sparkType)
    }

    ValidationResult(rejectedRDD, rejectedInputLinesRDD, acceptedRDD)
  }
}
