package com.ebiznext.comet.job.validator

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.model.{Attribute, Format, Type}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

object AcceptAllValidator extends GenericRowValidator {

  override def validate(
    session: SparkSession,
    format: Format,
    separator: String,
    dataset: DataFrame,
    attributes: List[Attribute],
    types: List[Type],
    sparkType: StructType
  )(implicit settings: Settings): (RDD[String], RDD[Row]) = {
    val rejectedRDD: RDD[String] = session.emptyDataFrame.rdd.map(_.mkString)
    val acceptedRDD: RDD[Row] = dataset.rdd
    (rejectedRDD, acceptedRDD)
  }
}
