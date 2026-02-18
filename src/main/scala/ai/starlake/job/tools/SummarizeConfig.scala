package ai.starlake.job.tools

import ai.starlake.job.ReportFormatConfig

case class SummarizeConfig(
  domain: String = "",
  table: String = "",
  accessToken: Option[String] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
