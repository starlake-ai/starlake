package ai.starlake.schema

case class ProjectCompareConfig(
  project1: String = "",
  project2: String = "",
  template: Option[String] = None,
  output: Option[String] = None
)
