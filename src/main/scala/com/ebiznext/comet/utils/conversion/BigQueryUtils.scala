package com.ebiznext.comet.utils.conversion

import com.google.cloud.bigquery.{Field, FieldList, StandardSQLTypeName, Schema => BQSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
  * [X] whatever
  * Conversion between [X] Schema and BigQuery Schema
  */
object BigQueryUtils {

  implicit val sparkToBq: Convertible[DataFrame, BQSchema] = new Convertible[DataFrame, BQSchema] {
    override def apply(v1: DataFrame): BQSchema = bqSchema(v1.schema)
  }

  def convert(sparkType: DataType): StandardSQLTypeName = {

    val BQ_NUMERIC_PRECISION = 38
    val BQ_NUMERIC_SCALE = 9
    lazy val NUMERIC_SPARK_TYPE =
      DataTypes.createDecimalType(BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE)

    sparkType match {
      case BooleanType                                     => StandardSQLTypeName.BOOL
      case ByteType | LongType | IntegerType               => StandardSQLTypeName.INT64
      case DoubleType | FloatType                          => StandardSQLTypeName.FLOAT64
      case StringType                                      => StandardSQLTypeName.STRING
      case BinaryType                                      => StandardSQLTypeName.BYTES
      case DateType                                        => StandardSQLTypeName.DATE
      case TimestampType                                   => StandardSQLTypeName.TIMESTAMP
      case DecimalType.SYSTEM_DEFAULT | NUMERIC_SPARK_TYPE => StandardSQLTypeName.NUMERIC
      case _                                               => throw new IllegalArgumentException(s"Unsupported type:$sparkType")
    }
  }

  /**
    *
    * Compute BigQuery Schema from Spark or PArquet Schema while Schema.bqSchema compute it from YMl File
    * @param schema Spark DataType
    * @return
    */

  def bqSchema(schema: DataType): BQSchema = {

    import scala.collection.JavaConverters._
    def inferBqSchema(
      field: String,
      dataType: DataType,
      nullable: Boolean,
      metadata: Metadata
    ): Field = {
      (field, dataType, nullable, metadata) match {
        case (field: String, dataType: ArrayType, _, _) =>
          val elementTypes: Seq[(String, DataType, Boolean, Metadata)] = fieldsSchemaAsMap(
            dataType.elementType
          )
          val arrayFields = elementTypes.map {
            case (name, dataType, nullable, metadata) =>
              Field
                .newBuilder(
                  name,
                  convert(dataType)
                )
                .setMode(if (nullable) {
                  Field.Mode.NULLABLE
                } else {
                  Field.Mode.REQUIRED
                })
                .build()
          }.asJava
          Field
            .newBuilder(
              field,
              StandardSQLTypeName.STRUCT,
              FieldList.of(arrayFields)
            )
            .setMode(Field.Mode.REPEATED)
            .build()
        case (field: String, dataType: DataType, nullable: Boolean, metadata: Metadata) =>
          Field
            .newBuilder(
              field,
              convert(dataType)
            )
            .setMode(if (nullable) {
              Field.Mode.NULLABLE
            } else {
              Field.Mode.REQUIRED
            })
            .build()
      }
    }

    val fields = fieldsSchemaAsMap(schema)
      .map {
        case (field, dataType, nullable, metadata) =>
          inferBqSchema(field, dataType, nullable, metadata)

      }
    BQSchema.of(fields: _*)
  }

  /**
    *
    * The aim of this function is to retrieve columns and nested columns
    * with their types from a spark schema
    * @param schema Spark Schema
    * @return List of Spark Columns with their Type
    */
  private def fieldsSchemaAsMap(schema: DataType): List[(String, DataType, Boolean, Metadata)] = {
    val fullName: String => String = name => name
    schema match {
      case StructType(fields) =>
        fields.toList.flatMap {
          case StructField(name, inner: StructType, nullable, metadata) =>
            (fullName(name), inner, nullable, metadata) +: fieldsSchemaAsMap(inner)
          case StructField(name, inner: ArrayType, nullable, metadata) =>
            (fullName(name), inner, nullable, metadata) +: fieldsSchemaAsMap(inner.elementType)
          case StructField(name, inner: DataType, nullable, metadata) =>
            List[(String, DataType, Boolean, Metadata)]((fullName(name), inner, nullable, metadata))
        }
      case _ => List.empty[(String, DataType, Boolean, Metadata)]
    }
  }

}
