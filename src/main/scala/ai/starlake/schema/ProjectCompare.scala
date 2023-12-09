package ai.starlake.schema

import ai.starlake.config.Settings
import ai.starlake.schema.model.{Project, ProjectDiff}
import org.apache.hadoop.fs.Path
import org.fusesource.scalate.{TemplateEngine, TemplateSource}

import scala.util.Try

object ProjectCompare {
  def run(args: Array[String])(implicit settings: Settings): Try[Unit] = Try {
    ProjectCompareConfig.parse(args) match {
      case Some(config) =>
        compare(config)
      case None =>
        throw new IllegalArgumentException(ProjectCompareConfig.usage())
    }
  }

  def compare(config: ProjectCompareConfig)(implicit settings: Settings): Unit = {
    val diff = Project.compare(config)
    val result = applyTemplate(diff, config.template)
    config.output match {
      case None =>
        println(result)
      case Some(output) =>
        settings.storageHandler().write(result, new Path(output))
    }
  }

  def applyTemplate(
    diff: ProjectDiff,
    templatePath: Option[String]
  )(implicit settings: Settings): String = {
    val engine: TemplateEngine = new TemplateEngine
    val (path, content) = templatePath match {
      case None =>
        val stream = getClass.getResourceAsStream("/templates/project-compare.ssp")
        val templateContent = scala.io.Source.fromInputStream(stream).mkString
        ("/templates/project-compare.ssp", templateContent)
      case Some(path) =>
        val templateContent = settings.storageHandler().read(new Path(path))
        (path, templateContent)
    }
    engine.layout(
      TemplateSource.fromText(path, content),
      Map("diff" -> diff)
    )
  }

}
