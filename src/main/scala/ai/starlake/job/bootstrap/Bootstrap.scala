package ai.starlake.job.bootstrap

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.utils.JarUtil
import better.files.File
import com.typesafe.scalalogging.StrictLogging

import scala.io.Source
import scala.util.Try

object Bootstrap extends StrictLogging {
  val SAMPLES_DIR = "bootstrap/samples"
  val TEMPLATES_DIR = s"$SAMPLES_DIR/templates"
  private def copyToFolder(
    resources: List[String],
    templateFolder: String,
    targetFolder: File
  ): Unit = {
    resources.foreach { resource =>
      logger.info(s"copying $resource")
      val source = Source.fromResource(resource)
      if (source == null)
        throw new Exception(s"Resource $resource not found in assembly")

      val lines: Iterator[String] = source.getLines()
      val targetFile =
        File(
          targetFolder.pathAsString,
          resource.substring(templateFolder.length + 1).split('/'): _*
        )
      targetFile.parent.createDirectories()
      val contents = lines.mkString("\n")
      targetFile.overwrite(contents)
    }
  }

  def askTemplate(maybeString: Option[String]): Option[String] = {
    maybeString match {
      case Some(template) =>
        Some(template)
      case None =>
        println("Please choose a template:")
        val templates = JarUtil.getResourceFolders("bootstrap/samples/templates")
        templates.zipWithIndex.foreach { case (template, index) =>
          println(s"  $index. $template")
        }
        println(s"  q. quit")

        var ok = false
        var template: Option[String] = None
        do {
          print(s"=> ")
          val input = scala.io.StdIn.readLine()
          if (input.toLowerCase() == "q") {
            template = None
            ok = true
          } else {
            val index = Try(input.toInt).getOrElse(-1)
            if (index < 0 || index >= templates.length) {
              println(s"Invalid input: $input")
              ok = false
            } else {
              template = Some(templates(index))
            }
          }
        } while (!ok)
        template
    }
  }

  def bootstrap(inputTemplate: Option[String])(implicit settings: Settings): Unit = {
    val template = askTemplate(inputTemplate)
    val rootFolder = File(DatasetArea.metadata.toString).parent
    rootFolder.createDirectories()
    if (rootFolder.collectChildren(!_.isDirectory).nonEmpty) {
      println(s"Folder ${rootFolder.pathAsString} already exists and not empty. Aborting.")
      System.exit(1)
    }
    askTemplate(template)
      .foreach { template =>
        val templatePath = s"$TEMPLATES_DIR/$template"
        val bootstrapFiles = JarUtil.getResourceFiles(templatePath)
        copyToFolder(bootstrapFiles, templatePath, rootFolder)
        val vsCodeDir = s"$SAMPLES_DIR/vscode"
        val vscodeExtensionFiles = List(s"$vsCodeDir/extensions.json")
        val targetDir = rootFolder / ".vscode"
        targetDir.createDirectories()
        copyToFolder(vscodeExtensionFiles, vsCodeDir, targetDir)
      }
  }
  def main(args: Array[String]): Unit = {
    // askTemplate(None)
    val template = "bigquery"
    val templatePath = s"$TEMPLATES_DIR/$template"
    val bootstrapFiles = JarUtil.getResourceFiles(templatePath)
    bootstrapFiles.foreach(println)
  }
}
