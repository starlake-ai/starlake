package com.ebiznext.comet.utils.conversion

import com.ebiznext.comet.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.{StandardSQLTypeName, Schema => BQSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/** [X] whatever
  * Conversion between [X] Schema and BigQuery Schema
  */
object BigQueryUtils {

  implicit val sparkToBq: Convertible[DataFrame, BQSchema] = (df: DataFrame) => bqSchema(df.schema)

  // from com.google.cloud.spark.bigquery.SchemaConverters.convertByBigQueryType
  def convert(sparkType: DataType): StandardSQLTypeName = {

    val BQ_NUMERIC_PRECISION = 38
    val BQ_NUMERIC_SCALE = 9
    lazy val NUMERIC_SPARK_TYPE: DecimalType =
      DataTypes.createDecimalType(BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE)

    sparkType match {
      case BinaryType                                         => StandardSQLTypeName.BYTES
      case ByteType | ShortType | IntegerType | LongType      => StandardSQLTypeName.INT64
      case BooleanType                                        => StandardSQLTypeName.BOOL
      case DoubleType | FloatType                             => StandardSQLTypeName.FLOAT64
      case DecimalType.SYSTEM_DEFAULT | NUMERIC_SPARK_TYPE    => StandardSQLTypeName.NUMERIC
      case StringType                                         => StandardSQLTypeName.STRING
      case TimestampType                                      => StandardSQLTypeName.TIMESTAMP
      case DateType                                           => StandardSQLTypeName.DATE
      case StructType(_) | ArrayType(_, _) | MapType(_, _, _) => StandardSQLTypeName.STRUCT
      case _                                                  => throw new IllegalArgumentException(s"Unsupported type:$sparkType")
    }
  }

  /** Compute BigQuery Schema from Spark or PArquet Schema while Schema.bqSchema compute it from YMl File
    * @param schema Spark DataType
    * @return
    */

  def bqSchema(schema: DataType): BQSchema = {
    BigQuerySchemaConverters.toBigQuerySchema(schema.asInstanceOf[StructType])
  }
}
