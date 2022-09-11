package ai.starlake.job.sink

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataFrameTransform {
  def transform(dataFrame: DataFrame, session: SparkSession): DataFrame
}

object DataFrameTransform {
  def transform(
    transformInstance: Option[DataFrameTransform],
    df: DataFrame,
    session: SparkSession
  ): DataFrame = {
    val transformedDF = transformInstance match {
      case Some(transformer) =>
        transformer.transform(df, session)
      case None =>
        df
    }
    transformedDF
  }

}
