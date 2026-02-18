package ai.starlake.job.ingest

import ai.starlake.job.ReportFormatConfig

case class AutoLoadConfig(
  domains: Seq[String] = Seq.empty,
  tables: Seq[String] = Seq.empty,
  options: Map[String, String] = Map.empty,
  clean: Boolean = false,
  accessToken: Option[String],
  scheduledDate: Option[String],
  reportFormat: Option[String] = None
) extends ReportFormatConfig
