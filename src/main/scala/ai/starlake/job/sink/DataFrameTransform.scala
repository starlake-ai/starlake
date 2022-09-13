package ai.starlake.job.sink

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataFrameTransform {
  def transform(dataFrame: DataFrame, session: SparkSession): DataFrame
}

object IdentityDataFrameTransformer extends DataFrameTransform {
  override def transform(dataFrame: DataFrame, session: SparkSession): DataFrame = dataFrame
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
