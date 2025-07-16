package ai.starlake.job.validator

import ai.starlake.config.{CometColumns, PrivacyLevels}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Attributes, Format, TableAttribute, Type}
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{functions, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object TreeRowValidator extends GenericRowValidator {

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
    val validator = new RowValidator(
      attributes,
      types,
      PrivacyLevels.allPrivacyLevels(privacyOptions),
      emptyIsNull,
      rejectWithValue
    )
    val (rejectedDF, acceptedDF) =
      validator.validateWorkflow(dataset, Attributes.from(dataset.schema))

    val formattedRejectedDF = rejectedDF
      .select(
        functions
          .concat(
            lit("ERR -> \n"),
            concat_ws("\n", col(RowValidator.SL_ERROR_COL)),
            lit("\n\nFILE -> "),
            col(CometColumns.cometInputFileNameColumn)
          )
          .as("errors"),
        col(CometColumns.cometInputFileNameColumn).as("path")
      )
      .as[SimpleRejectedRecord]

    CheckValidityResult(
      formattedRejectedDF.persist(cacheStorageLevel),
      rejectedDF.select(col(RowValidator.SL_INPUT_COL + ".*")).persist(cacheStorageLevel),
      acceptedDF.persist(cacheStorageLevel)
    )
  }
}
