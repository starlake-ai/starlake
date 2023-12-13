package ai.starlake.schema.generator

import better.files.File

case class AutoTaskDependenciesConfig(
  outputFile: Option[File] = None,
  tasks: Option[Seq[String]] = None,
  reload: Boolean = false,
  objects: Seq[String] = Seq("task", "table"),
  viz: Boolean = false,
  print: Boolean = false,
  svg: Boolean = false,
  png: Boolean = false,
  all: Boolean = false
)
