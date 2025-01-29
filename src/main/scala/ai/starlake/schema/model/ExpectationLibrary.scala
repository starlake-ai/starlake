package ai.starlake.schema.model

import ai.starlake.config.{DatasetArea, Settings}
import org.apache.hadoop.fs.Path

object ExpectationLibrary {
  def categories()(implicit settings: Settings): List[String] = {
    val path = DatasetArea.expectations
    settings.storageHandler().list(path, recursive = false).map(_.path.getName)
  }
  def expectations(category: String)(implicit settings: Settings): List[String] = {
    val path = DatasetArea.expectations
    val catPath = new Path(path, category + ".j2")
    val data = settings.storageHandler().read(catPath)
    val regex = """\{%\s*macro\s+(.*)\s*%}""".r
    val macros = regex.findAllMatchIn(data).map(_.group(1).trim).toList
    macros
  }
}
