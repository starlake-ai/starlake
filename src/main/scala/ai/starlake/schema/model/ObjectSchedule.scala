package ai.starlake.schema.model

case class ObjectSchedule(
  domain: String,
  table: String,
  cron: String,
  comment: Option[String] = None
)
