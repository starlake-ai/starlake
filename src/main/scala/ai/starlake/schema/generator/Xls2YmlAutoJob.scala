package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
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
      .foreach { autotask =>
        val taskPath = File(basePath, autotask.domain)
        logger.info(s"Generating autoJob schema for ${autotask.name} in $taskPath")
        writeAutoTaskYaml(
          autotask,
          taskPath,
          autotask.name
        )
      }
  }

  def writeAutoTaskYaml(autotask: AutoTaskDesc, outputPath: File, fileName: String): Unit = {
    outputPath.createIfNotExists(asDirectory = true, createParents = true)
    logger.info(s"""Generated autoJob schemas:
                   |${serialize(autotask)}""".stripMargin)
    serializeToFile(File(outputPath, s"$fileName.sl.yml"), autotask)
  }

  def run(args: Array[String]): Try[Unit] = Try {
    implicit val settings: Settings = Settings(Settings.referenceConfig)
    Xls2YmlConfig.parse(args) match {
      case Some(config) =>
        config.files.foreach(generateSchema(_, config.policyFile, config.outputPath))
      case _ =>
        throw new IllegalArgumentException(Xls2YmlConfig.usage())
    }
  }

  def main(args: Array[String]): Unit = {
    Xls2YmlAutoJob.run(args)
    System.exit(0)
  }
}
