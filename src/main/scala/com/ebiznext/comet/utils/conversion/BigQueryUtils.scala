package com.ebiznext.comet.utils.conversion

import com.google.cloud.bigquery.{StandardSQLTypeName, Schema => BQSchema}
import com.google.cloud.spark.bigquery.SchemaConverters
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/** [X] whatever
  * Conversion between [X] Schema and BigQuery Schema
  */
object BigQueryUtils {

  val schemaConverter = new SchemaConverters

  implicit val sparkToBq: Convertible[DataFrame, BQSchema] = new Convertible[DataFrame, BQSchema] {
    override def apply(v1: DataFrame): BQSchema = bqSchema(v1.schema)
  }

  def convert(sparkType: DataType): StandardSQLTypeName = {

    val BQ_NUMERIC_PRECISION = 38
    val BQ_NUMERIC_SCALE = 9
    lazy val NUMERIC_SPARK_TYPE =
      DataTypes.createDecimalType(BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE)

    sparkType match {
      case BooleanType                                        => StandardSQLTypeName.BOOL
      case ByteType | LongType | IntegerType                  => StandardSQLTypeName.INT64
      case DoubleType | FloatType                             => StandardSQLTypeName.FLOAT64
      case StringType                                         => StandardSQLTypeName.STRING
      case BinaryType                                         => StandardSQLTypeName.BYTES
      case DateType                                           => StandardSQLTypeName.DATE
      case TimestampType                                      => StandardSQLTypeName.TIMESTAMP
      case DecimalType.SYSTEM_DEFAULT | NUMERIC_SPARK_TYPE    => StandardSQLTypeName.NUMERIC
      case StructType(_) | ArrayType(_, _) | MapType(_, _, _) => StandardSQLTypeName.STRUCT
      case _                                                  => throw new IllegalArgumentException(s"Unsupported type:$sparkType")
    }
  }

  /** Compute BigQuery Schema from Spark or PArquet Schema while Schema.bqSchema compute it from YMl File
    * @param schema Spark DataType
    * @return
    */

  def bqSchema(schema: DataType): BQSchema = {
//
//    import scala.collection.JavaConverters._
//    def inferBqSchema(field: String, dataType: DataType): Field = {
//      (field, dataType) match {
//        case (field: String, dataType: ArrayType) =>
//          val builder: Field.Builder = convert(dataType.elementType) match {
//            case StandardSQLTypeName.STRUCT =>
//              val elementTypes: Seq[(String, DataType)] = fieldsSchemaAsMap(dataType.elementType)
//              val arrayFields = elementTypes.map { case (name, dataType) =>
//                Field
//                  .newBuilder(
//                    name,
//                    convert(dataType)
//                  )
//                  .setMode(Field.Mode.NULLABLE)
//                  .build()
//              }.asJava
//              Field
//                .newBuilder(
//                  field,
//                  convert(dataType.elementType),
//                  FieldList.of(arrayFields)
//                )
//            case bqDatatype =>
//              Field
//                .newBuilder(
//                  field,
//                  bqDatatype
//                )
//
//          }
//
//          builder
//            .setMode(Field.Mode.REPEATED)
//            .setDescription("")
//            .build()
//        case (field: String, dataType: StructType) =>
//          val converted = dataType.fields.map { attr =>
//            inferBqSchema(attr.name, attr.dataType)
//          }
//          Field
//            .newBuilder(
//              field,
//              StandardSQLTypeName.STRUCT,
//              FieldList.of(converted.toList.asJava)
//            )
//            .setMode(Field.Mode.NULLABLE)
//            .setDescription("")
//            .build()
//        case (field: String, dataType: DataType) =>
//          Field
//            .newBuilder(
//              field,
//              convert(dataType)
//            )
//            .setMode(Field.Mode.NULLABLE)
//            .setDescription("")
//            .build()
//      }
//    }
//
//    val fields = fieldsSchemaAsMap(schema)
//      .map { case (field, dataType) =>
//        inferBqSchema(field, dataType)
//
//      }
//    BQSchema.of(fields: _*)

    SchemaConverters.toBigQuerySchema(schema.asInstanceOf[StructType])

  }

  /** The aim of this function is to retrieve columns and nested columns
    * with their types from a spark schema
    * @param schema Spark Schema
    * @return List of Spark Columns with their Type
    */
  private def fieldsSchemaAsMap(schema: DataType): List[(String, DataType)] = {
    schema match {
      case StructType(fields) =>
        fields.toList.flatMap {
          case StructField(name, inner: StructType, _, _) =>
            (name, inner) +: fieldsSchemaAsMap(inner)
          case StructField(name, inner: ArrayType, _, _) =>
            (name, inner) +: fieldsSchemaAsMap(inner.elementType)
          case StructField(name, inner: DataType, _, _) =>
            List[(String, DataType)]((name, inner))
        }
      case _ => List.empty[(String, DataType)]
    }
  }

}
