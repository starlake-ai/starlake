package ai.starlake.semantic

import ai.starlake.job.ReportFormatConfig

case class SemanticExportConfig(
  format: String = "ossie",
  model: Option[String] = None,
  output: Option[String] = None,
  connection: Option[String] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
