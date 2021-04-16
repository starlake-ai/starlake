package com.ebiznext.comet.job.validator

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.job.ingest.IngestionUtil
import com.ebiznext.comet.schema.model.Rejection.{ColInfo, ColResult, RowInfo, RowResult}
import com.ebiznext.comet.schema.model.{Attribute, Type}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.time.Instant

object FlatRowValidator extends GenericRowValidator {

  override def validate(
    session: SparkSession,
    dataset: DataFrame,
    attributes: List[Attribute],
    types: List[Type],
    sparkType: StructType
  )(implicit settings: Settings): (RDD[String], RDD[Row]) = {

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
              }.toList
            )
          } else {
            RowResult(
              rowCols.map { case ((colRawValue, colAttribute), tpe) =>
                IngestionUtil.validateCol(colRawValue, colAttribute, tpe, colMap)
              }.toList
            )
          }
        }
      } persist (settings.comet.cacheStorageLevel)

    val rejectedRDD: RDD[String] = checkedRDD
      .filter(_.isRejected)
      .map(rr => RowInfo(now, rr.colResults.filter(!_.colInfo.success).map(_.colInfo)).toString)

    val acceptedRDD: RDD[Row] = checkedRDD.filter(_.isAccepted).map { rowResult =>
      val sparkValues: List[Any] = rowResult.colResults.map(_.sparkValue)
      new GenericRowWithSchema(Row(sparkValues: _*).toSeq.toArray, sparkType)
    }

    (rejectedRDD, acceptedRDD)
  }
}
