package ai.starlake.job.ingest

case class AutoLoadConfig(
  domains: Seq[String] = Seq.empty,
  tables: Seq[String] = Seq.empty,
  options: Map[String, String] = Map.empty,
  clean: Boolean = false,
  accessToken: Option[String],
  scheduledDate: Option[String]
)
