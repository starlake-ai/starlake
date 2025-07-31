package ai.starlake.job.bootstrap

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.utils.{JarUtil, YamlSerde}
import better.files.File
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging

import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.util.Try

object Bootstrap extends LazyLogging {
  val TEMPLATES_DIR = "templates/bootstrap/samples"
  private def copyToFolder(
    resources: List[String],
    templateFolder: String,
    targetFolder: File
  ): List[File] = {
    resources.filterNot(_.endsWith(".DS_Store")).map { resource =>
      logger.info(s"copying $resource")
      val source = Source.fromResource(resource)
      if (source == null)
        throw new Exception(s"Resource $resource not found in assembly")

      val lines: Iterator[String] = source.getLines()
      val targetFile =
        File(
          targetFolder.pathAsString,
          resource.substring(templateFolder.length).split('/').toIndexedSeq: _*
        )
      targetFile.parent.createDirectories()
      val contents = lines.mkString("\n")
      targetFile.overwrite(contents)
    }
  }

  private def askTemplate(maybeString: Option[String]): Option[String] = {
    maybeString match {
      case Some(template) =>
        Some(template)
      case None =>
        val templates = JarUtil.getResourceFolders("templates/bootstrap/samples/")
        if (templates.length == 1) {
          Some(templates.head)
        } else {

          println("Please choose a template:")
          templates.zipWithIndex.foreach { case (template, index) =>
            println(s"  $index. $template")
          }
          println(s"  q. quit")
          requestAnswer(templates)
        }
    }
  }

  private def requestAnswer(choices: List[String]): Option[String] = {
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
        if (index < 0 || index >= choices.length) {
          println(s"Invalid input: $input")
          ok = false
        } else {
          template = Some(choices(index))
          ok = true
        }
      }
    } while (!ok)
    template
  }

  def bootstrap(inputTemplate: Option[String])(implicit settings: Settings): Unit = {
    val template = askTemplate(inputTemplate)
    val metadataFolder = File(DatasetArea.metadata.toString)
    metadataFolder.createDirectories()
    File(metadataFolder.parent, "datasets").createDirectories()
    if (metadataFolder.collectChildren(!_.isDirectory).nonEmpty) {
      println(s"Folder ${metadataFolder.pathAsString} already exists and is not empty. Aborting.")
      System.exit(1)
    }
    DatasetArea.initMetadata(settings.storageHandler())
    // val dagLibDir = File(metadataFolder, "dags", "generated")

    val dagLibDir = File(File(DatasetArea.build.toString), "dags")
    dagLibDir.createDirectories()

    template
      .foreach { template =>
        val rootFolder = metadataFolder.parent
        val templatePath = s"$TEMPLATES_DIR/$template/"

        // copy template files
        val bootstrapFiles = JarUtil.getResourceFiles(templatePath)
        copyToFolder(bootstrapFiles, templatePath, rootFolder)

        // copy vscode settings
        val vscodeExtensionFiles = List(s"$TEMPLATES_DIR/extensions.json")
        val targetDir = rootFolder / ".vscode"
        targetDir.createDirectories()
        copyToFolder(vscodeExtensionFiles, TEMPLATES_DIR, targetDir)

        // copy gitignore
        val gitIgnoreFilename = List(s"$TEMPLATES_DIR/gitignore")
        val gitIgnoreFile = copyToFolder(gitIgnoreFilename, TEMPLATES_DIR, targetDir).head
        val dotDitIgnoreFile = gitIgnoreFile.parent / ".gitignore"
        gitIgnoreFile.moveTo(dotDitIgnoreFile)(File.CopyOptions(overwrite = true))

        if (template == "initializer") {
          val appFile = metadataFolder / "application.sl.yml"

          val contents = appFile.contentAsString
          val rootNode = YamlSerde.mapper.readTree(contents)
          val appNode = rootNode.path("application").asInstanceOf[ObjectNode]
          val connectionsNode = appNode.path("connections").asInstanceOf[ObjectNode]

          val connectionKeys =
            connectionsNode.fieldNames.asScala.toList

          println("Please choose a connection type:")
          connectionKeys.zipWithIndex.foreach { case (template, index) =>
            println(s"  $index. $template")
          }
          requestAnswer(connectionKeys).foreach { connectionName =>
            connectionKeys.foreach { key =>
              if (key != connectionName)
                connectionsNode.remove(key)
            }
            if (connectionName == "bigquery") {
              val loaders = List(
                "spark (full features)",
                "native (limited to validation features provided by the datawarehouse)"
              )
              println("Select loader:")
              loaders.zipWithIndex.foreach { case (template, index) =>
                println(s"  $index. $template")
              }
              requestAnswer(loaders).foreach { loader =>
                appNode.put("loader", loader.split(' ').head)
                if (loader.startsWith("native") || connectionName != "bigquery") {
                  appNode.remove("spark")
                }
              }
            }
            appFile.overwrite(YamlSerde.mapper.writeValueAsString(rootNode))
          }
        }
      }
  }

  def main(args: Array[String]): Unit = {
    // askTemplate(None)
    val template = "bigquery"
    val templatePath = s"bootstrap/$template"
    val bootstrapFiles = JarUtil.getResourceFiles(templatePath)
    bootstrapFiles.foreach(println)
  }
}
