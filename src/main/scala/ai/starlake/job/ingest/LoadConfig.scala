package ai.starlake.job.ingest

case class LoadConfig(
  domains: Seq[String] = Seq.empty,
  tables: Seq[String] = Seq.empty,
  options: Map[String, String] = Map.empty,
  accessToken: Option[String],
  test: Boolean,
  files: Option[List[String]] = None,
  variant: Option[Boolean] = None,
  primaryKey: List[String] = List.empty,
  scheduledDate: Option[String]
)
