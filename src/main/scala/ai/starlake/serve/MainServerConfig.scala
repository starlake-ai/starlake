package ai.starlake.serve

import ai.starlake.job.ReportFormatConfig

case class MainServerConfig(
  host: Option[String] = None,
  port: Option[Int] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
