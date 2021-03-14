package com.ebiznext.comet.job.validator

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.job.ingest.IngestionUtil
import com.ebiznext.comet.schema.model.{Attribute, Type}
import com.ebiznext.comet.utils.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

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
    * @param sparkType  : The expected Spark Type for valid rows
    * @return Two RDDs : One RDD for rejected rows and one RDD for accepted rows
    */
  override def validate(
    session: SparkSession,
    dataset: DataFrame,
    attributes: List[Attribute],
    types: List[Type],
    sparkType: StructType
  )(implicit settings: Settings): (RDD[String], RDD[Row]) = {
    val typesMap = types.map(tpe => tpe.name -> tpe).toMap
    /*

    val resulRDD = dataset.rdd.flatMap { row =>
      val rowWithSchema = row.asInstanceOf[GenericRowWithSchema]
      traverseRow(rowWithSchema, Utils.toMap(attributes), sparkType, typesMap)
    }
//    val rejectedRDD = resulRDD.filter(!_._2).map(_._1.toSeq.head.toString)
//    val acceptedRDD = resulRDD.filter(_._2).map(_._1.asInstanceOf[Row])
    val rejectedRDD = session.sparkContext.parallelize(Seq(""))
    val acceptedRDD = resulRDD
    (rejectedRDD, acceptedRDD)
     */
    val withPrivacyDF = run(dataset, attributes, sparkType, typesMap)
    val errRDD = session.sparkContext.parallelize(List.empty[String])
    (errRDD, withPrivacyDF.rdd)
  }

  def run(
    acceptedDF: DataFrame,
    attributes: List[Attribute],
    sparkType: StructType,
    typesMap: Map[String, Type]
  )(implicit
    settings: Settings
  ): DataFrame = {
    implicit val encoder = acceptedDF.encoder
    acceptedDF.map { row =>
      val rowWithSchema = row.asInstanceOf[GenericRowWithSchema]
      traverseRowBis(rowWithSchema, Utils.toMap(attributes), sparkType, typesMap)
    }
  }

//  def traverseRow(
//    row: GenericRowWithSchema,
//    attributes: Map[String, Any],
//    sparkType: StructType,
//    types: Map[String, Type]
//  )(implicit
//    settings: Settings
//  ): Option[Row] = {
//    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
//    var ok = true
//    def lift[T](res: Option[T]) = res match {
//      case Some(x) =>
//        x
//      case None =>
//        ok = false
//        null
//    }
//    def lift[T](res: Either[String, Any]) = res match {
//      case Right(x) =>
//        x
//      case Left(err) =>
//        ok = false
//        errorList += err
//        null
//    }
//    def validateCol(attribute: Attribute, item: Any): Either[String, Any] = {
//      val colResult = IngestionUtil.validateCol(
//        Option(item).map(_.toString),
//        attribute,
//        types(attribute.`type`),
//        Map.empty[String, Option[String]]
//      )
//      colResult.colInfo.success match {
//        case true  => Right(colResult.sparkValue)
//        case false => Left(colResult.colInfo.toString)
//      }
//    }
//
//    val cells = row.toSeq.zip(row.schema.fields.map(_.name))
//
//    val updatedRow: Seq[Any] = cells.map {
//      case (cell: GenericRowWithSchema, name) =>
//        lift(traverseRow(cell, attributes(name).asInstanceOf[Map[String, Any]], sparkType, types))
//      case (cell: mutable.WrappedArray[_], name) =>
//        cell.map {
//          case subcell: GenericRowWithSchema =>
//            lift(
//              traverseRow(
//                subcell,
//                attributes(name).asInstanceOf[Map[String, Any]],
//                sparkType,
//                types
//              )
//            )
//          case subcell =>
//            lift(validateCol(attributes(name).asInstanceOf[Attribute], subcell))
//        }
//      case (cell, "comet_input_file_name") =>
//        cell
//      case (cell, name) =>
//        lift(validateCol(attributes(name).asInstanceOf[Attribute], cell))
//    }
//    if (errorList.isEmpty) {
//      val created = new GenericRowWithSchema(
//        updatedRow.toArray,
//        StructType(row.schema.fields :+ StructField("comet_err", StringType, nullable = true))
//      )
//      created
//    } else {
//      val updatedRowWithErr = updatedRow :+ errorList.mkString(",")
//      val created = new GenericRowWithSchema(
//        updatedRowWithErr.toArray,
//        StructType(row.schema.fields :+ StructField("comet_err", StringType, nullable = true))
//      )
//    }
//  }

  def traverseRowBis(
    row: GenericRowWithSchema,
    attributes: Map[String, Any],
    sparkType: StructType,
    types: Map[String, Type]
  )(implicit
    settings: Settings
  ): GenericRowWithSchema = {
    def validateCol(attribute: Attribute, item: Any): Any = {
      val colResult = IngestionUtil.validateCol(
        Option(item).map(_.toString),
        attribute,
        types(attribute.`type`),
        Map.empty[String, Option[String]]
      )
      colResult.colInfo.success match {
        case true  => colResult.sparkValue
        case false => null //TODO Should raise an error here.
      }
    }

    val cells = row.toSeq.zip(row.schema.fields.map(_.name))
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty

    val updatedRow: Seq[Any] = cells.map {
      case (cell: GenericRowWithSchema, name) =>
        traverseRowBis(
          cell,
          attributes(name).asInstanceOf[Map[String, Any]],
          sparkType,
          types
        )
      case (cell: mutable.WrappedArray[_], name) =>
        cell.map {
          case subcell: GenericRowWithSchema =>
            traverseRowBis(
              subcell,
              attributes(name).asInstanceOf[Map[String, Any]],
              sparkType,
              types
            )
          case subcell =>
            validateCol(attributes(name).asInstanceOf[Attribute], subcell)
        }
      case (cell, "comet_input_file_name") =>
        cell
      case (null, name) =>
        null
      case (cell, name) =>
        validateCol(attributes(name).asInstanceOf[Attribute], cell)
    }
    if (errorList.isEmpty) {
      new GenericRowWithSchema(updatedRow.toArray, sparkType)
    } else {
      //TODO: We should return an error here
      new GenericRowWithSchema(updatedRow.toArray, sparkType)
    }
  }
}
