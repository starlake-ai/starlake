package ai.starlake.job.validator

import ai.starlake.schema.model.{Attribute, Type}
import ai.starlake.config.Settings
import ai.starlake.schema.model.{Attribute, Format, Type}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

case class ValidationResult(
  errors: RDD[String],
  rejected: RDD[String],
  accepted: RDD[Row]
)

trait GenericRowValidator {

  /** For each col of each row
    *   - we extract the col value / the col constraints / col type
    *   - we check that the constraints are verified
    *   - we apply any required privacy transformation
    *   - parse the column into the target primitive Spark Type We end up using catalyst to create a
    *     Spark Row
    *
    * @param session
    *   : The Spark session
    * @param dataset
    *   : The dataset
    * @param attributes
    *   : the col attributes
    * @param types
    *   : List of globally defined types
    * @param sparkType
    *   : The expected Spark Type for valid rows
    * @return
    *   Two RDDs : One RDD for rejected rows and one RDD for accepted rows
    */
  def validate(
    session: SparkSession,
    format: Format,
    separator: String,
    dataset: DataFrame,
    attributes: List[Attribute],
    types: List[Type],
    sparkType: StructType
  )(implicit settings: Settings): ValidationResult
}
