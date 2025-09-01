package ai.starlake.lineage

import better.files.File
case class ColLineageConfig(
  task: String,
  outputFile: Option[File] = None,
  sql: Option[String] = None,
  accessToken: Option[String] = None
)
