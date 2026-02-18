package ai.starlake.config

import ai.starlake.job.ReportFormatConfig

case class SettingsConfig(
  testConnection: Option[String],
  reportFormat: Option[String] = None
) extends ReportFormatConfig
