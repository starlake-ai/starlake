package ai.starlake.job.metrics

import ai.starlake.job.ReportFormatConfig

case class MetricsConfig(
  domain: String = "",
  schema: String = "",
  authInfo: Map[String, String] = Map.empty,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
