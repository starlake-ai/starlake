package com.ebiznext.comet.utils.conversion

import com.ebiznext.comet.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.{Schema => BQSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/** [X] whatever
  * Conversion between [X] Schema and BigQuery Schema
  */
object BigQueryUtils {

  val sparkToBq: DataFrame => BQSchema = (df: DataFrame) => bqSchema(df.schema)

  /** Compute BigQuery Schema from Spark or PArquet Schema while Schema.bqSchema compute it from YMl File
    * @param schema Spark DataType
    * @return
    */

  def bqSchema(schema: DataType): BQSchema = {
    BigQuerySchemaConverters.toBigQuerySchema(schema.asInstanceOf[StructType])
  }
}
