package ai.starlake.job.validator

import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Format, TableAttribute, Type}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/** Never directly called. Act as a marker for Native Loaders
  */
object NativeValidator extends GenericRowValidator {
  override def validate(
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
  )(implicit schemaHandler: SchemaHandler): CheckValidityResult = {
    import session.implicits._
    val rejectedDS = session.emptyDataset[SimpleRejectedRecord]
    val rejectedInputDS = session.emptyDataFrame
    val acceptedDS = dataset
    CheckValidityResult(rejectedDS, rejectedInputDS, acceptedDS)
  }
}
