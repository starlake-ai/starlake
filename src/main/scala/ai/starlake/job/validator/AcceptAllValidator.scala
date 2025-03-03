package ai.starlake.job.validator

import ai.starlake.config.PrivacyLevels
import ai.starlake.job.validator.RowValidator.{SL_ERROR_COL, SL_INPUT_COL}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Attribute, Attributes, Format, Type}
import org.apache.spark.sql.functions.{array_size, col}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object AcceptAllValidator extends GenericRowValidator {
  override def validate(
    session: SparkSession,
    format: Format,
    separator: String,
    dataset: DataFrame,
    attributes: List[Attribute],
    types: List[Type],
    sparkType: StructType,
    privacyOptions: Map[String, String],
    cacheStorageLevel: StorageLevel,
    sinkReplayToFile: Boolean,
    emptyIsNull: Boolean,
    rejectWithValue: Boolean
  )(implicit schemaHandler: SchemaHandler): CheckValidityResult = {
    import session.implicits._
    val rejectedDS = session.emptyDataset[String]
    val rejectedInputDS = session.emptyDataFrame
    val validator = new RowValidator(
      attributes,
      types,
      PrivacyLevels.allPrivacyLevels(privacyOptions),
      emptyIsNull,
      rejectWithValue
    )
    val fileAttributes = Attributes.from(dataset.schema)
    val fittedDF =
      dataset
        .transform(validator.prepareData(fileAttributes))
        .transform(validator.fitToSchema(fileAttributes))
    CheckValidityResult(rejectedDS, rejectedInputDS, fittedDF)
  }
}
