package ai.starlake.job.ingest

case class StageConfig(
  domains: Seq[String] = Seq.empty,
  options: Map[String, String] = Map.empty
)
