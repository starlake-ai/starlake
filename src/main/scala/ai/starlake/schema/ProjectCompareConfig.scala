package ai.starlake.schema

case class ProjectCompareConfig(
  path1: String = "",
  path2: String = "",
  gitWorkTree: String = "",
  commit1: String = "",
  commit2: String = "",
  tag1: String = "",
  tag2: String = "",
  template: Option[String] = None,
  output: Option[String] = None
)
