package ai.starlake.schema.model

trait PartitionInfo
case class FieldPartitionInfo(
  field: String,
  expirationDays: Option[Int],
  requirePartitionFilter: Boolean
) extends PartitionInfo
