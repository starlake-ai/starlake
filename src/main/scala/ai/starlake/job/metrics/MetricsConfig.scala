package ai.starlake.job.metrics

case class MetricsConfig(
  domain: String = "",
  schema: String = "",
  authInfo: Map[String, String] = Map.empty
)
