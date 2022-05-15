package ai.starlake.job.validator

import ai.starlake.config.{CometColumns, PrivacyLevels}
import ai.starlake.job.ingest.IngestionUtil
import ai.starlake.schema.model.Rejection.{ColInfo, ColResult, RowInfo, RowResult}
import ai.starlake.schema.model.{Attribute, Format, Type}
import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import java.time.Instant

object FlatRowValidator extends GenericRowValidator {

  private def toOriginalFormat(row: Row, format: Format, separator: String): String = {
    format match {
      case Format.DSV =>
        // dropRight removes CometInputFileName Column
        row.toSeq.dropRight(1).map(Option(_).getOrElse("").toString).mkString(separator)
      case Format.SIMPLE_JSON =>
        val rowAsMap = row.getValuesMap(row.schema.fieldNames)
        new Gson.toJson(rowAsMap - CometColumns.cometInputFileNameColumn)
      case Format.POSITION =>
        // dropRight removes CometInputFileName Column
        row.toSeq.dropRight(1).map(_.toString).mkString("")
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
    sparkType: StructType,
    privacyOptions: Map[String, String],
    cacheStorageLevel: StorageLevel,
    sinkReplayToFile: Boolean
  ): ValidationResult = {
    val now = Timestamp.from(Instant.now)
    val checkedRDD: RDD[RowResult] = dataset.rdd
      .map { row =>
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
          val colResults = rowCols.map { case ((colRawValue, colAttribute), tpe) =>
            ColResult(
              ColInfo(
                colRawValue,
                colAttribute.name,
                tpe.name,
                tpe.pattern,
                success = false
              ),
              ""
            )
          }.toList
          RowResult(
            colResults,
            false,
            row.getAs[String](CometColumns.cometInputFileNameColumn),
            Some(toOriginalFormat(row, format, separator))
          )
        } else {
          val colResults = rowCols.map { case ((colRawValue, colAttribute), tpe) =>
            IngestionUtil.validateCol(
              colRawValue,
              colAttribute,
              tpe,
              colMap,
              PrivacyLevels.allPrivacyLevels(privacyOptions)
            )
          }.toList
          val isRowAccepted = colResults.forall(_.colInfo.success)
          RowResult(
            colResults,
            isRowAccepted,
            row.getAs[String](CometColumns.cometInputFileNameColumn),
            if (isRowAccepted || !sinkReplayToFile) None
            else Some(toOriginalFormat(row, format, separator))
          )
        }
      } persist cacheStorageLevel

    val rejectedRDD = checkedRDD
      .filter(_.isRejected)
      .map(rowResult =>
        RowInfo(
          now,
          rowResult.colResults.filter(!_.colInfo.success).map(_.colInfo),
          rowResult.inputFileName
        ).toString
      )

    val rejectedInputLinesRDD = checkedRDD.filter(_.isRejected).flatMap(_.inputLine)

    implicit val enc = RowEncoder.apply(sparkType)
    val acceptedRDD: RDD[Row] = checkedRDD
      .filter(_.isAccepted)
      .map { rowResult =>
        val sparkValues: List[Any] = rowResult.colResults.map(_.sparkValue)
        // Row(sparkValues.toArray)
        new GenericRowWithSchema(sparkValues.toArray, sparkType)

      }

    val acceptedDS = session.createDataFrame(acceptedRDD, sparkType)

    import session.implicits._
    val rejectedDS = rejectedRDD.toDS()
    val rejectedInputLinesDS = rejectedInputLinesRDD.toDS()

    ValidationResult(
      rejectedDS persist cacheStorageLevel,
      rejectedInputLinesDS persist cacheStorageLevel,
      acceptedDS persist cacheStorageLevel
    )
  }
}
