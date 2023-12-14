package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerializer._
import better.files.File
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

object Xls2YmlAutoJob extends LazyLogging {

  def generateSchema(
    inputPath: String,
    policyPath: Option[String] = None,
    outputPath: Option[String] = None
  )(implicit
    settings: Settings
  ): Unit = {
    val basePath = outputPath.getOrElse(DatasetArea.transform.toString)
    val reader = new XlsAutoJobReader(InputPath(inputPath), policyPath.map(InputPath))
    reader.autoTasksDesc
      .foreach { autoTask =>
        val taskPath = File(basePath, autoTask.domain)
        logger.info(s"Generating autoJob schema for ${autoTask.name} in $taskPath")
        writeAutoTaskYaml(
          autoTask,
          taskPath,
          autoTask.name
        )
      }
  }

  def writeAutoTaskYaml(autoTask: AutoTaskDesc, outputPath: File, fileName: String): Unit = {
    outputPath.createIfNotExists(asDirectory = true, createParents = true)
    logger.info(s"""Generated autoJob schemas:
                   |${serialize(autoTask)}""".stripMargin)
    serializeToFile(File(outputPath, s"$fileName.sl.yml"), autoTask)
  }

  def run(args: Array[String]): Try[Unit] = {
    implicit val settings: Settings = Settings(Settings.referenceConfig)
    Xls2YmlAutoJobCmd.run(args, new SchemaHandler(settings.storageHandler())).map(_ => ())
  }

  def main(args: Array[String]): Unit = {
    Xls2YmlAutoJob.run(args)
    System.exit(0)
  }
}
