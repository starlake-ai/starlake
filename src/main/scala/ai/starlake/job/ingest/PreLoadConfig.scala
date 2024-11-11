package ai.starlake.job.ingest

case class PreLoadConfig(
  domain: String,
  tables: Seq[String] = Seq.empty,
  strategy: Option[PreLoadStrategy] = None,
  options: Map[String, String] = Map.empty,
  accessToken: Option[String]
)
