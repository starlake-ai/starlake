package ai.starlake.schema.handlers

import ai.starlake.job.ReportFormatConfig

case class ValidateConfig(reload: Boolean = false, reportFormat: Option[String] = None)
    extends ReportFormatConfig
