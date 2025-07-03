package ai.starlake.schema.model

case class ObjectSchedule(
  domain: String,
  table: String,
  cron: Option[String],
  typ: String,
  comment: Option[String] = None
)
