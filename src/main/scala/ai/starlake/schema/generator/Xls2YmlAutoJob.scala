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
    val reader = new XlsAutoJobReader(InputPath(inputPath), policyPath.map(InputPath))
    reader.autoJobsDesc
      .filter(_.tasks.exists(_.attributesDesc.nonEmpty))
      .foreach { autojob =>
        writeAutoJobYaml(
          autojob,
          outputPath.getOrElse(DatasetArea.jobs.toString),
          autojob.name
        )
      }
  }

  def writeAutoJobYaml(autoJobDesc: AutoJobDesc, outputPath: String, fileName: String)(implicit
    settings: Settings
  ): Unit = {
    logger.info(s"""Generated autoJob schemas:
                   |${serialize(autoJobDesc)}""".stripMargin)
    serializeToFile(File(outputPath, s"$fileName.comet.yml"), autoJobDesc)
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
