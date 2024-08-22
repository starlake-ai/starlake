package ai.starlake.lineage

import better.files.File
case class ColLineageConfig(task: String, outputFile: Option[File] = None)
