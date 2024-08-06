package ai.starlake.job.ingest

case class LoadConfig(
  domains: Seq[String] = Seq.empty,
  tables: Seq[String] = Seq.empty,
  options: Map[String, String] = Map.empty,
  test: Boolean,
  files: Option[List[String]] = None
)
