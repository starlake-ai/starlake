package ai.starlake.job.validator

import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Format, TableAttribute, Type}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

case class CheckValidityResult(
  errors: Dataset[SimpleRejectedRecord],
  rejected: Dataset[Row],
  accepted: Dataset[Row]
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
    *   Two dataframe : One dataframe for rejected rows and one dataframe for accepted rows
    */
  def validate(
    session: SparkSession,
    format: Format,
    separator: String,
    dataset: DataFrame,
    attributes: List[TableAttribute],
    types: List[Type],
    sparkType: StructType,
    privacyOptions: Map[String, String],
    cacheStorageLevel: StorageLevel,
    sinkReplayToFile: Boolean,
    emptyIsNull: Boolean,
    rejectWithValue: Boolean
  )(implicit schemaHandler: SchemaHandler): CheckValidityResult
}
