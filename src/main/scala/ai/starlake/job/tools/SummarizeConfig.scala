package ai.starlake.job.tools

case class SummarizeConfig(
  domain: String = "",
  table: String = "",
  accessToken: Option[String] = None
)
