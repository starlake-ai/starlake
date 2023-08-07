package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerializer._
import better.files.File
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

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
    serializeToFile(File(outputPath, s"$fileName.comet.yml"), autotask)
  }

  def run(args: Array[String]): Boolean = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    Xls2YmlConfig.parse(args) match {
      case Some(config) =>
        config.files.foreach(generateSchema(_, config.policyFile, config.outputPath))
        true
      case _ =>
        println(Xls2YmlConfig.usage())
        false
    }
  }

  def main(args: Array[String]): Unit = {
    val result = Xls2YmlAutoJob.run(args)
    System.exit(if (result) 0 else 1)
  }
}
