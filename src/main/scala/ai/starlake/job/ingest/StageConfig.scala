package ai.starlake.job.ingest

import ai.starlake.job.ReportFormatConfig

case class StageConfig(
  domains: Seq[String] = Seq.empty,
  tables: Seq[String] = Seq.empty,
  options: Map[String, String] = Map.empty,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
