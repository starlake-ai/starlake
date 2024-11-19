package ai.starlake.job.ingest

case class PreLoadConfig(
  domain: String,
  tables: Seq[String] = Seq.empty,
  strategy: Option[PreLoadStrategy] = None,
  globalAckFilePath: Option[String] = None,
  options: Map[String, String] = Map.empty
)
