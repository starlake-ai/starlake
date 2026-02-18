package ai.starlake.console

import ai.starlake.job.ReportFormatConfig

case class ConsoleConfig(
  options: Map[String, String] = Map.empty,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
