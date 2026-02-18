package ai.starlake.schema.generator

import ai.starlake.job.ReportFormatConfig

case class DagDeployConfig(
  inputDir: Option[String] = None,
  outputDir: String,
  dagDir: Option[String] = None,
  clean: Boolean = false,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
