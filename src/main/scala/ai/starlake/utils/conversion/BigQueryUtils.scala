package ai.starlake.utils.conversion

import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.{Schema => BQSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.types._

/** [X] whatever Conversion between [X] Schema and BigQuery Schema
  */
object BigQueryUtils {

  val sparkToBq: DataFrame => BQSchema = (df: DataFrame) => bqSchema(df.schema)

  /** Compute BigQuery Schema from Spark or PArquet Schema while Schema.bqSchema compute it from YMl
    * File
    * @param schema
    *   Spark DataType
    * @return
    */

  def bqSchema(schema: DataType): BQSchema = {
    BigQuerySchemaConverters.toBigQuerySchema(schema.asInstanceOf[StructType])
  }

  /** Spark BigQuery driver consider integer in BQ as Long. We need to convert the Int DataType to
    * LongType before loading the data. As a good practice, always use long when dealing with big
    * query in your YAML Schema.
    * @param schema
    * @return
    */
  def normalizeSchema(schema: StructType): StructType = {
    val fields = schema.fields.map { field =>
      field.dataType match {
        case dataType: StructType =>
          field.copy(dataType = normalizeSchema(dataType))
        case ArrayType(elementType: StructType, nullable) =>
          field.copy(dataType = ArrayType(normalizeSchema(elementType), nullable))
        case ArrayType(_: IntegerType, nullable) =>
          field.copy(dataType = ArrayType(LongType, nullable))
        case IntegerType => field.copy(dataType = LongType)
        case _           => field
      }
    }
    schema.copy(fields = fields)
  }

  def computePartitionsToUpdateAfterMerge(
    mergedDF: DataFrame,
    toDeleteDF: DataFrame,
    timestamp: String
  ): List[String] = {
    mergedDF
      .select(col(timestamp))
      .union(toDeleteDF.select(col(timestamp)))
      .select(date_format(col(timestamp), "yyyyMMdd").cast("string"))
      .where(col(timestamp).isNotNull)
      .distinct()
      .collect()
      .map(_.getString(0))
      .toList
  }

}
