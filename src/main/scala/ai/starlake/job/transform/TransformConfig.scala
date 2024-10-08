package ai.starlake.job.transform

case class TransformConfig(
  name: String = "",
  options: Map[String, String] = Map.empty,
  compile: Boolean = false,
  tags: Seq[String] = Seq.empty,
  format: Boolean = false,
  interactive: Option[String] = None,
  reload: Boolean = false,
  truncate: Boolean = false,
  recursive: Boolean = false,
  test: Boolean = false,
  accessToken: Option[String] = None,
  dryRun: Boolean = false,
  query: Option[String] = None
)
