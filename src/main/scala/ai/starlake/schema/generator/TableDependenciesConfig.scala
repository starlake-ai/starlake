package ai.starlake.schema.generator

import better.files.File

case class TableDependenciesConfig(
  includeAllAttributes: Boolean = true,
  related: Boolean = false,
  outputFile: Option[File] = None,
  tables: Option[Seq[String]] = None,
  reload: Boolean = false,
  svg: Boolean = false,
  png: Boolean = false,
  all: Boolean = true
)
