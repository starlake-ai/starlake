package ai.starlake.schema.model

case class JDBCSource(
  config: String,
  schemas: List[Map[String, List[String]]]
)
