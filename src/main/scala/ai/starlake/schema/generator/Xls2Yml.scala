package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerializer._
import better.files.File
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

object Xls2Yml extends LazyLogging {

  def writeDomainsAsYaml(inputPath: String, outputPath: Option[String] = None)(implicit
    settings: Settings
  ): Unit = {
    val reader = new XlsDomainReader(InputPath(inputPath))
    reader.getDomain().foreach { domain =>
      writeDomainAsYaml(domain, File(outputPath.getOrElse(DatasetArea.load.toString)))
    }
  }

  def writeDomainAsYaml(domain: Domain, basePath: File): Unit = {
    logger.info(s"""Generated schemas:
         |${serialize(domain)}""".stripMargin)
    domain.writeDomainAsYaml(basePath)
  }

  def writeIamPolicyTagsAsYaml(
    iamPolicyTags: IamPolicyTags,
    outputPath: String,
    fileName: String
  ): Unit = {
    logger.info(s"""Generated schemas:
         |${serialize(iamPolicyTags)}""".stripMargin)
    serializeToFile(File(outputPath, s"${fileName}.sl.yml"), iamPolicyTags)
  }

  def run(args: Array[String]): Try[Boolean] = {
    implicit val settings: Settings = Settings(Settings.referenceConfig)
    Xls2YmlCmd.run(args, new SchemaHandler(settings.storageHandler())).map(_ => true)
  }

  def main(args: Array[String]): Unit = {
    Xls2Yml.run(args)
  }
}

object Main {

  def main(args: Array[String]): Unit = {
    println(
      "[deprecated]: Please use ai.starlake.schema.generator.Xls2Yml instead of ai.starlake.schema.generator.Main"
    )
    Thread.sleep(10000)
    Xls2Yml.main(args)
  }
}
