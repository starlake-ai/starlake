package ai.starlake.schema

import ai.starlake.config.Settings
import ai.starlake.schema.model.{Project, ProjectDiff}
import ai.starlake.utils.Utils
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.util
import scala.util.Try

object ProjectCompare {
  def run(args: Array[String])(implicit settings: Settings): Try[Unit] =
    ProjectCompareCmd.run(args.toIndexedSeq, settings.schemaHandler()).map(_ => ())

  private def applyJ2AndSave(
    outputDir: Path,
    templateContent: String,
    context: util.HashMap[String, Object],
    filename: String
  )(implicit settings: Settings): Unit = {
    val paramMap = Map(
      "context" -> context
    )
    val jinjaOutput = Utils.parseJinjaTpl(templateContent, paramMap)
    val path = new Path(outputDir, filename)
    settings.storageHandler().write(jinjaOutput, path)(StandardCharsets.UTF_8)
  }

  def compare(config: ProjectCompareConfig)(implicit settings: Settings): Unit = {
    val diff: ProjectDiff = Project.compare(config)
    println(Utils.newJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(diff))
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
    val content = templatePath match {
      case None =>
        val stream = getClass.getResourceAsStream("/templates/compare/index.html.j2")
        val templateContent = scala.io.Source.fromInputStream(stream).mkString
        templateContent
      case Some(path) =>
        val templateContent = settings.storageHandler().read(new Path(path))
        templateContent
    }
    Utils.parseJinjaTpl(
      content,
      Map[String, Object](
        "diff"      -> diff,
        "timestamp" -> DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now())
      )
    )
  }

}
