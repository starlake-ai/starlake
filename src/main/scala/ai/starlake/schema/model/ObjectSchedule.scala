package ai.starlake.schema.model

case class ObjectSchedule(
  domain: String,
  table: String,
  cron: String,
  typ: String,
  comment: Option[String] = None
)
