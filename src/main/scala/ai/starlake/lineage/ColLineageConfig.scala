package ai.starlake.lineage

import ai.starlake.job.ReportFormatConfig
import better.files.File
case class ColLineageConfig(
  task: String,
  outputFile: Option[File] = None,
  sql: Option[String] = None,
  accessToken: Option[String] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
