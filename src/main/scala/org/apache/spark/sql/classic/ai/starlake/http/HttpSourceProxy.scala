package org.apache.spark.sql.classic.ai.starlake.http

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.types.StructType

class HttpSourceProxy {
  def internalCreateDataFrame(
    session: SparkSession,
    rdd: RDD[InternalRow],
    schema: StructType,
    isStreaming: Boolean = false
  ): DataFrame = {
    session.internalCreateDataFrame(
      rdd,
      schema,
      isStreaming
    )
  }
}
