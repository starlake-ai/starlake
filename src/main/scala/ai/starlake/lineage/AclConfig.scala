package ai.starlake.lineage

import ai.starlake.job.ReportFormatConfig

case class AclConfig(
  `export`: Boolean = false,
  outputPath: String = "",
  reportFormat: Option[String] = None
) extends ReportFormatConfig