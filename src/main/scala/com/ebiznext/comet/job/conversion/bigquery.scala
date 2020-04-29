package com.ebiznext.comet.job.conversion

import com.google.cloud.bigquery.{Field, LegacySQLTypeName, Schema => BQSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
  * [X] whatever
  * Conversion between [X] Schema and BigQuery Schema
  */
object bigquery {

  implicit val sparkToBq: Convertible[DataFrame, BQSchema] = new Convertible[DataFrame, BQSchema] {
    override def apply(v1: DataFrame): BQSchema = bqSchema(v1)
  }

  def convert(sparkType: DataType): LegacySQLTypeName = {

    val BQ_NUMERIC_PRECISION = 38
    val BQ_NUMERIC_SCALE = 9
    lazy val NUMERIC_SPARK_TYPE =
      DataTypes.createDecimalType(BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE)

    sparkType match {
      case BooleanType                                     => LegacySQLTypeName.BOOLEAN
      case ByteType | LongType | IntegerType               => LegacySQLTypeName.INTEGER
      case DoubleType | FloatType                          => LegacySQLTypeName.FLOAT
      case StringType                                      => LegacySQLTypeName.STRING
      case BinaryType                                      => LegacySQLTypeName.BYTES
      case DateType                                        => LegacySQLTypeName.DATE
      case TimestampType                                   => LegacySQLTypeName.TIMESTAMP
      case DecimalType.SYSTEM_DEFAULT | NUMERIC_SPARK_TYPE => LegacySQLTypeName.NUMERIC
      case _                                               => throw new IllegalArgumentException(s"Unsupported type:$sparkType")
    }
  }

  /**
    *
    * @param df Spark DataFrame
    * @return
    */
  private[conversion] def bqSchema(df: DataFrame): BQSchema = {
    val fields = fieldsSchemaAsMap(df.schema)
      .map {
        case (field: String, dataType: DataType) =>
          Field
            .newBuilder(
              field,
              convert(dataType)
            )
            .setMode(Field.Mode.NULLABLE)
            .setDescription("")
            .build()
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
  private def fieldsSchemaAsMap(schema: DataType): List[(String, DataType)] = {
    val fullName: String => String = name => name
    schema match {
      case StructType(fields: Array[StructField]) =>
        fields.toList.flatMap {
          case StructField(name, inner: StructType, _, _) =>
            (fullName(name), inner) +: fieldsSchemaAsMap(inner)
          case StructField(name, inner: ArrayType, _, _) =>
            (fullName(name), inner) +: fieldsSchemaAsMap(inner.elementType)
          case StructField(name, inner: DataType, _, _) =>
            List[(String, DataType)]((fullName(name), inner))
        }
      case _ => List.empty[(String, DataType)]
    }
  }

}
