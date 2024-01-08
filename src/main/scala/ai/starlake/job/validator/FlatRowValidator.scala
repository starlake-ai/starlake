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
        new Gson().toJson(rowAsMap - CometColumns.cometInputFileNameColumn)
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
    sinkReplayToFile: Boolean,
    emptyIsNull: Boolean
  ): ValidationResult = {
    val now = Timestamp.from(Instant.now)
    val attributesAndTypes = attributes.zip(types).toArray
    val attributesLen = attributes.length
    val checkedRDD: RDD[RowResult] = dataset.rdd
      .map { row =>
        val rowSeq = row.toSeq
        val validNumberOfColumns = attributes.length <= rowSeq.length
        if (!validNumberOfColumns) {
          val rowValues: Seq[(Option[String], Attribute, Type)] = rowSeq
            .zip(attributesAndTypes)
            .map { case (colValue, (colAttribute, colType)) =>
              (Option(colValue.asInstanceOf[String]), colAttribute, colType)
            }
          val colResults = rowValues.map { case (colRawValue, colAttribute, tpe) =>
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
          }
          RowResult(
            colResults,
            false,
            row.getAs[String](CometColumns.cometInputFileNameColumn),
            Some(toOriginalFormat(row, format, separator))
          )
        } else {
          var isRowAccepted = true
          val colResults = new Array[ColResult](attributesLen)
          lazy val colMap: Map[String, Option[String]] = {
            val result = new Array[(String, Option[String])](attributesLen)
            for (i <- rowSeq.indices) {
              val colValue = Option(rowSeq(i)_.toString)
              val (colAttribute, colType) = attributesAndTypes(i)
              result.update(i, (colAttribute.name, colValue))
            }
            result.toMap
          }

          for (i <- rowSeq.indices) {
            val colValue = Option(rowSeq(i)).map(_.toString)
            val (colAttribute, colType) = attributesAndTypes(i)
            val colResult = IngestionUtil.validateCol(
              colValue,
              colAttribute,
              colType,
              colMap,
              PrivacyLevels.allPrivacyLevels(privacyOptions),
              emptyIsNull
            )
            isRowAccepted = colResult.colInfo.success && isRowAccepted
            colResults.update(i, colResult)
          }
          RowResult(
            colResults,
            isRowAccepted,
            row.getAs[String](CometColumns.cometInputFileNameColumn),
            if (isRowAccepted || !sinkReplayToFile) None
            else Some(toOriginalFormat(row, format, separator))
          )
        }
      } // persist cacheStorageLevel

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

    implicit val enc = RowEncoder.encoderFor(sparkType)
    val acceptedRDD: RDD[Row] = checkedRDD
      .filter(_.isAccepted)
      .map { rowResult =>
        val sparkValues: Seq[Any] = rowResult.colResults.map(_.sparkValue)
        // Row(sparkValues.toArray)
        new GenericRowWithSchema(sparkValues.toArray, sparkType)

      }

    val acceptedDS = session.createDataFrame(acceptedRDD, sparkType)

    import session.implicits._
    val rejectedDS = rejectedRDD.toDS()
    val rejectedInputLinesDS = rejectedInputLinesRDD.toDS()

    ValidationResult(
      rejectedDS.persist(cacheStorageLevel),
      rejectedInputLinesDS.persist(cacheStorageLevel),
      acceptedDS.persist(cacheStorageLevel)
    )
  }
}
