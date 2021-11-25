package ai.starlake.job.validator

import ai.starlake.config.Settings
import ai.starlake.schema.model.{Attribute, Format, Type}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object AcceptAllValidator extends GenericRowValidator {

  override def validate(
    session: SparkSession,
    format: Format,
    separator: String,
    dataset: DataFrame,
    attributes: List[Attribute],
    types: List[Type],
    sparkType: StructType
  )(implicit settings: Settings): ValidationResult = {
    import session.implicits._
    val rejectedDS = session.emptyDataset[String]
    val rejectedInputDS = session.emptyDataset[String]
    val acceptedDS = dataset
    ValidationResult(rejectedDS, rejectedInputDS, acceptedDS)
  }
}
