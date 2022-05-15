/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */
package org.apache.spark.sql.execution.datasources.json

import ai.starlake.utils.Utils
import com.fasterxml.jackson.core.JsonToken._
import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.numericPrecedence
import org.apache.spark.sql.catalyst.json.JacksonUtils
import org.apache.spark.sql.types._

import java.util.Comparator
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import JsonParser.NumberType._
import com.fasterxml.jackson.core.JsonParser.Feature

/** Code here comes from org.apache.spark.sql.execution.datasources.json.InferSchema
  */
object JsonIngestionUtil {

  private val structFieldComparator = new Comparator[StructField] {

    override def compare(o1: StructField, o2: StructField): Int = {
      o1.name.compare(o2.name)
    }
  }

  def compareTypes(schemaType: DataType, datasetType: DataType): List[String] = {
    compareTypes(Nil, ("root", schemaType, true), ("root", datasetType, true))
  }

  /** similar to compatibleType(...) but instead of creating a new datatype, simply check the
    * compatibility
    *
    * @param context
    *   : full path to attribute, makes error messages more understandable
    * @param schemaType
    *   : (attributeName, attributeType, isRequired) coming from the schema
    * @param datasetType:
    *   (attributeName, attributeType, isRequired) coming from the dataset
    * @return
    *   List of errors, Nil when datasetType is compatible with schemaType
    */
  def compareTypes(
    context: List[String],
    schemaType: (String, DataType, Boolean),
    datasetType: (String, DataType, Boolean)
  ): List[String] = {
    val (schemaAttrName, schemaAttrType, schemaAttrNullable) = schemaType
    val (datasetAttrName, datasetAttrType, datasetAttrNullable) = datasetType
    val schemaTypeNullable: Boolean = schemaAttrNullable
    (schemaAttrType, datasetAttrType) match {
      case (t1, t2) if t1 == t2                                              => Nil
      case (_, NullType) if schemaTypeNullable                               => Nil
      case (_: IntegralType, _: IntegralType)                                => Nil
      case (_: FractionalType, _: IntegralType)                              => Nil
      case (_: FractionalType, _: FractionalType)                            => Nil
      case (_: TimestampType, _: DateType) | (_: DateType, _: TimestampType) => Nil
      case (_: TimestampType, _) | (_: DateType, _)                          =>
        // timestamp and date are validated against stirng and long (epochmillis)
        // We never reject the input here.
        Nil
      case (_: StringType, _: StringType) => Nil
      case (StructType(unsortedFields1), StructType(unsortedFields2)) =>
        val fields1 = unsortedFields1.sortBy(_.name)
        val fields2 = unsortedFields2.sortBy(_.name)
        val errorList: mutable.MutableList[String] = mutable.MutableList.empty
        var f1Idx = 0
        var f2Idx = 0
        var typeComp = true
        while (f1Idx < fields1.length && f2Idx < fields2.length && typeComp) {
          val f1 = fields1(f1Idx)
          val f2 = fields2(f2Idx)
          val nameComp = f1.name.compareTo(f2.name)
          if (nameComp < 0 && f1.nullable) {
            // Field exists in schema  and is not present in the input record
            // go get the next field in the schema
            f1Idx += 1
          } else if (nameComp == 0) {
            // field is present in the schema and the input record : check that types are equal
            val f1Type = f1.dataType
            val f2Type = f2.dataType
            errorList ++= compareTypes(
              context :+ schemaAttrName,
              (f1.name, f1Type, f1.nullable),
              (f2.name, f2Type, f2.nullable)
            )
            f1Idx += 1
            f2Idx += 1
            typeComp = typeComp && errorList.isEmpty
          } else {
            // Field is present in the message but not in the schema.
            addError(context, errorList, f2)
            typeComp = false
          }
        }
        if (f1Idx == fields1.length) {
          // Field is present in the message but not in the schema.
          while (f2Idx < fields2.length) {
            val f2 = fields2(f2Idx)
            addError(context, errorList, f2)
            f2Idx += 1
          }
        }

        errorList.toList
      case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, _)) =>
        compareTypes(
          context :+ schemaAttrName,
          (schemaAttrName, elementType1, containsNull1),
          (schemaAttrName, elementType2, containsNull1)
        )
      case (_, _) =>
        List(
          s"Validation error in context: ${context
              .mkString(".")}, $datasetAttrName:$datasetAttrType isnullable:$datasetAttrNullable against " +
          s"schema $schemaAttrName:$schemaAttrType isnullable:$schemaAttrNullable"
        )
    }
  }

  private def addError(
    context: List[String],
    errorList: mutable.MutableList[String],
    f2: StructField
  ): Unit = {
    errorList += s"""${f2.name}, ${f2.dataType.typeName}, ${context.mkString(
        "."
      )}, unknown field ${f2.name} : ${f2.dataType.typeName} in context ${context
        .mkString(".")}"""
  }

// From Spark TypeCoercion
  val findTightestCommonTypeOfTwo: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1)       => Some(t1)
    case (t1, NullType)       => Some(t1)

    case (t1: IntegralType, t2: DecimalType) if t2.isWiderThan(t1) =>
      Some(t2)
    case (t1: DecimalType, t2: IntegralType) if t1.isWiderThan(t2) =>
      Some(t1)

    // Promote numeric types to the highest of the two
    case (t1: NumericType, t2: NumericType)
        if !t1.isInstanceOf[DecimalType] && !t2.isInstanceOf[DecimalType] =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case (_: TimestampType, _: DateType) | (_: DateType, _: TimestampType) =>
      Some(TimestampType)

    case _ => None
  }

  def compatibleType(t1: DataType, t2: DataType): DataType = {
    findTightestCommonTypeOfTwo(t1, t2).getOrElse {
      (t1, t2) match {
        case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) =>
          DoubleType

        case (t1: DecimalType, t2: DecimalType) =>
          val scale = math.max(t1.scale, t2.scale)
          val range = math.max(t1.precision - t1.scale, t2.precision - t2.scale)
          if (range + scale > 38) {
            // DecimalType can't support precision > 38
            DoubleType
          } else {
            DecimalType(range + scale, scale)
          }

        case (StructType(unsortedFields1), StructType(unsortedFields2)) =>
          val fields1 = unsortedFields1.sortBy(_.name)
          val fields2 = unsortedFields2.sortBy(_.name)
          val newFields = new java.util.ArrayList[StructField]
          var f1Idx = 0
          var f2Idx = 0

          while (f1Idx < fields1.length && f2Idx < fields2.length) {
            val f1Name = fields1(f1Idx).name
            val f2Name = fields2(f2Idx).name
            val comp = f1Name.compareTo(f2Name)
            if (comp == 0) {
              val dataType =
                compatibleType(fields1(f1Idx).dataType, fields2(f2Idx).dataType)
              newFields.add(StructField(f1Name, dataType, nullable = true))
              f1Idx += 1
              f2Idx += 1
            } else if (comp < 0) { // f1Name < f2Name
              newFields.add(fields1(f1Idx))
              f1Idx += 1
            } else { // f1Name > f2Name
              newFields.add(fields2(f2Idx))
              f2Idx += 1
            }
          }
          while (f1Idx < fields1.length) {
            newFields.add(fields1(f1Idx))
            f1Idx += 1
          }
          while (f2Idx < fields2.length) {
            newFields.add(fields2(f2Idx))
            f2Idx += 1
          }
          StructType(newFields.toArray(Array.empty[StructField]))

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)

        // The case that given `DecimalType` is capable of given `IntegralType` is handled in
        // `findTightestCommonTypeOfTwo`. Both cases below will be executed only when
        // the given `DecimalType` is not capable of the given `IntegralType`.
        case (t1: IntegralType, t2: DecimalType) =>
          compatibleType(DecimalType.forType(t1), t2)
        case (t1: DecimalType, t2: IntegralType) =>
          compatibleType(t1, DecimalType.forType(t2))

        // strings and every string is a Json object.
        case (_, _) => StringType
      }
    }
  }

  /** Convert NullType to StringType and remove StructTypes with no fields
    */
  private def canonicalizeType(tpe: DataType): Option[DataType] =
    tpe match {
      case at @ ArrayType(elementType, _) =>
        for {
          canonicalType <- canonicalizeType(elementType)
        } yield {
          at.copy(canonicalType)
        }

      case StructType(fields) =>
        val canonicalFields: Array[StructField] = for {
          field <- fields
          if field.name.nonEmpty
          canonicalType <- canonicalizeType(field.dataType)
        } yield {
          field.copy(dataType = canonicalType)
        }

        if (canonicalFields.length > 0) {
          Some(StructType(canonicalFields))
        } else {
          // per SPARK-8093: empty structs should be deleted
          None
        }

      case NullType => Some(StringType)
      case other    => Some(other)
    }

//  private def withCorruptField(
//    struct: StructType,
//    columnNameOfCorruptRecords: String
//  ): StructType = {
//    if (!struct.fieldNames.contains(columnNameOfCorruptRecords)) {
//      // If this given struct does not have a column used for corrupt records,
//      // add this field.
//      val newFields: Array[StructField] =
//        StructField(columnNameOfCorruptRecords, StringType, nullable = true) +: struct.fields
//      // Note: other code relies on this sorting for correctness, so don't remove it!
//      java.util.Arrays.sort(newFields, structFieldComparator)
//      StructType(newFields)
//    } else {
//      // Otherwise, just return this struct.
//      struct
//    }
//  }
//
//  /** Remove top-level ArrayType wrappers and merge the remaining schemas
//    */
//  private def compatibleRootType(
//    columnNameOfCorruptRecords: String,
//    shouldHandleCorruptRecord: Boolean
//  ): (DataType, DataType) => DataType = {
//    // Since we support array of json objects at the top level,
//    // we need to check the element type and find the root level data type.
//    case (ArrayType(ty1, _), ty2) =>
//      compatibleRootType(columnNameOfCorruptRecords, shouldHandleCorruptRecord)(ty1, ty2)
//    case (ty1, ArrayType(ty2, _)) =>
//      compatibleRootType(columnNameOfCorruptRecords, shouldHandleCorruptRecord)(ty1, ty2)
//    // If we see any other data type at the root level, we get records that cannot be
//    // parsed. So, we use the struct as the data type and add the corrupt field to the schema.
//    case (struct: StructType, NullType) => struct
//    case (NullType, struct: StructType) => struct
//    case (struct: StructType, o) if !o.isInstanceOf[StructType] && shouldHandleCorruptRecord =>
//      withCorruptField(struct, columnNameOfCorruptRecords)
//    case (o, struct: StructType) if !o.isInstanceOf[StructType] && shouldHandleCorruptRecord =>
//      withCorruptField(struct, columnNameOfCorruptRecords)
//    // If we get anything else, we call compatibleType.
//    // Usually, when we reach here, ty1 and ty2 are two StructTypes.
//    case (ty1, ty2) => compatibleType(ty1, ty2)
//  }

  def inferSchema(parser: JsonParser): DataType = {
    parser.getCurrentToken match {
      case null | VALUE_NULL => NullType
      case FIELD_NAME =>
        parser.nextToken()
        inferSchema(parser)
      case START_ARRAY =>
        var elementType: DataType = NullType
        while (JacksonUtils.nextUntil(parser, END_ARRAY)) {
          elementType = compatibleType(elementType, inferSchema(parser))
        }
        ArrayType(elementType)
      case START_OBJECT =>
        val builder = Array.newBuilder[StructField]
        while (JacksonUtils.nextUntil(parser, END_OBJECT)) {
          builder += StructField(parser.getCurrentName, inferSchema(parser), nullable = true)
        }
        val fields: Array[StructField] = builder.result()
        StructType(fields)
      case VALUE_STRING =>
        StringType
      case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
        parser.getNumberType match {
          case INT | LONG =>
            LongType
          case BIG_INTEGER | BIG_DECIMAL =>
            val v = parser.getDecimalValue
            if (Math.max(v.precision(), v.scale()) <= DecimalType.MAX_PRECISION) {
              DecimalType(Math.max(v.precision(), v.scale()), v.scale())
            } else {
              DoubleType
            }
          case FLOAT | DOUBLE =>
            DoubleType
        }
      case VALUE_TRUE | VALUE_FALSE =>
        BooleanType
      case _ =>
        throw new Exception("Should never happen")
    }

  }

  def parseString(content: String): Try[DataType] = {
    Try {
      Utils.withResources(factory.createParser(content)) { parser =>
        parser.nextToken()
        JsonIngestionUtil.inferSchema(parser)
      }
    }
  }

  def parseRDD(
    inputRDD: RDD[Row],
    schemaSparkType: DataType
  ): RDD[Either[List[String], (String, String)]] = {
    inputRDD.mapPartitions { partition =>
      partition.map { row =>
        val rowAsString = row.getAs[String]("value")
        parseString(rowAsString) match {
          case Success(datasetType) =>
            val errorList = compareTypes(schemaSparkType, datasetType)
            if (errorList.isEmpty)
              Right((rowAsString, row.getAs[String]("input_file_name()")))
            else
              Left(errorList)

          case Failure(exception) =>
            Left(List(exception.toString))
        }
      }
    }
  }

  val factory = (new JsonFactory).enable(Feature.ALLOW_SINGLE_QUOTES)
}
