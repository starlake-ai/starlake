package ai.starlake.job.sink

import ai.starlake.config.Settings
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.spark.sql.functions.{array, col, lit, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConverters._

trait DataFrameTransform {
  def transform(dataFrame: DataFrame, session: SparkSession): DataFrame
}

object IdentityDataFrameTransformer extends DataFrameTransform {
  override def transform(dataFrame: DataFrame, session: SparkSession): DataFrame = dataFrame
}

object HeaderDataFrameTransformer extends DataFrameTransform {
  val avroSerializer = new KafkaAvroSerializer()
  val settings = Settings(ConfigFactory.load())
  avroSerializer.configure(settings.comet.kafka.serverOptions.asJava, false)
  override def transform(dataFrame: DataFrame, session: SparkSession): DataFrame = {
    import session.implicits._
    dataFrame
      .select(col("value"))
      .map(row => avroSerializer.serialize("test_http_kafka_load", row.getAs[String](0)))
      .withColumn(
        "headers",
        array(
          struct(
            lit("maclef") as "key",
            lit(avroSerializer.serialize("test_http_kafka_load", "ma valeur"))
              .cast("binary") as "value"
          ),
          struct(
            lit("taclef") as "key",
            lit(avroSerializer.serialize("test_http_kafka_load", "ta valeur"))
              .cast("binary") as "value"
          )
        )
      )
  }
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
