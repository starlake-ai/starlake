package com.ebiznext.comet.job.validator

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.job.ingest.IngestionUtil
import com.ebiznext.comet.schema.model.{Attribute, Type}
import com.ebiznext.comet.utils.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object TreeRowValidator extends GenericRowValidator {

  /** For each col of each row
    *   - we extract the col value / the col constraints / col type
    *   - we check that the constraints are verified
    *   - we apply any required privacy transformation
    *   - parse the column into the target primitive Spark Type
    *     We end up using catalyst to create a Spark Row
    *
    * @param session    : The Spark session
    * @param dataset    : The dataset
    * @param attributes : the col attributes
    * @param types      : List of globally defined types
    * @param schemaSparkType  : The expected Spark Type for valid rows
    * @return Two RDDs : One RDD for rejected rows and one RDD for accepted rows
    */
  override def validate(
    session: SparkSession,
    dataset: DataFrame,
    attributes: List[Attribute],
    types: List[Type],
    schemaSparkType: StructType
  )(implicit settings: Settings): (RDD[String], RDD[Row]) = {
    val typesMap = types.map(tpe => tpe.name -> tpe).toMap
    val successErrorRDD = validateDataset(session, dataset, attributes, schemaSparkType, typesMap)
    val successRDD: RDD[Row] =
      successErrorRDD
        .filter(row => row.getAs[Boolean](Settings.cometSuccessColumn))
        .map(row => new GenericRowWithSchema(row.toSeq.dropRight(2).toArray, schemaSparkType))

    val errorRDD: RDD[String] =
      successErrorRDD
        .filter(row => !row.getAs[Boolean](Settings.cometSuccessColumn))
        .map(row => row.getAs[String](Settings.cometErrorMessageColumn))
    (errorRDD, successRDD)
  }

  private def validateDataset(
    session: SparkSession,
    dataset: DataFrame,
    attributes: List[Attribute],
    schemaSparkType: StructType,
    typesMap: Map[String, Type]
  )(implicit
    settings: Settings
  ): RDD[Row] = {

    val schemaSparkTypeWithSuccessErrorMessage =
      StructType(
        schemaSparkType.fields ++ Array(
          StructField(Settings.cometSuccessColumn, BooleanType, false),
          StructField(Settings.cometErrorMessageColumn, StringType, false)
        )
      )
    //  implicit val encoder2 = RowEncoder(schemaSparkType)
    dataset.rdd.map { row =>
      val rowWithSchema = row.asInstanceOf[GenericRowWithSchema]
      validateRow(rowWithSchema, Utils.toMap(attributes), schemaSparkType, typesMap)
    }

  }

  private def validateRow(
    row: GenericRowWithSchema,
    attributes: Map[String, Any],
    schemaSparkType: StructType,
    types: Map[String, Type]
  )(implicit
    settings: Settings
  ): GenericRowWithSchema = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    def validateCol(attribute: Attribute, item: Any): Any = {
      val colResult = IngestionUtil.validateCol(
        Option(item).map(_.toString),
        attribute,
        types(attribute.`type`),
        Map.empty[String, Option[String]]
      )
      if (colResult.colInfo.success) {
        colResult.sparkValue
      } else {
        errorList += colResult.colInfo.toString
        null
      }
    }

    val cells = row.toSeq.zip(row.schema.fields.map(_.name))
    val schemaSparkTypeWithSuccessErrorMessage =
      StructType(
        schemaSparkType.fields ++ Array(
          StructField(Settings.cometSuccessColumn, BooleanType, false),
          StructField(Settings.cometErrorMessageColumn, StringType, false)
        )
      )

    def cellHandleTimestamp(cell: Any) = {
      cell match {
        case timestamp: Timestamp =>
          DateTimeFormatter.ISO_INSTANT.format(timestamp.toInstant)
        case _ => cell
      }
    }

    val updatedRow: Array[Any] =
      Try {
        cells.map {
          case (cell: GenericRowWithSchema, name) =>
            validateRow(
              cell,
              attributes(name).asInstanceOf[Map[String, Any]],
              schemaSparkType,
              types
            )
          case (cell: mutable.WrappedArray[_], name) =>
            cell.map {
              case subcell: GenericRowWithSchema =>
                validateRow(
                  subcell,
                  attributes(name).asInstanceOf[Map[String, Any]],
                  schemaSparkType,
                  types
                )
              case subcell =>
                validateCol(attributes(name).asInstanceOf[Attribute], cellHandleTimestamp(subcell))
            }
          case (cell, "comet_input_file_name") =>
            cell
          case (null, name) =>
            null
          case (cell, name) =>
            validateCol(attributes(name).asInstanceOf[Attribute], cellHandleTimestamp(cell))
        }
      }
        .map(_.toArray) match {
        case Success(res) => res
        case Failure(exception) =>
          errorList += s"Invalid Node ${Utils.exceptionAsString(exception)}"
          Array.empty[Any]
      }

    val updatedRowWithMessage =
      if (errorList.isEmpty)
        updatedRow ++ Array(true, "")
      else
        updatedRow ++ Array(false, errorList.mkString("\n"))
    new GenericRowWithSchema(updatedRowWithMessage, schemaSparkTypeWithSuccessErrorMessage)
  }
}
