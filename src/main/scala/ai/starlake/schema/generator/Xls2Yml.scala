package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerializer._
import better.files.File
import com.typesafe.config.ConfigFactory
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
    val folder = File(basePath, domain.name)
    folder.createIfNotExists(asDirectory = true, createParents = true)
    domain.tables foreach { schema =>
      serializeToFile(File(folder, s"${schema.name}.sl.yml"), schema)
    }
    val domainDataOnly = domain.copy(tables = Nil)
    serializeToFile(File(folder, s"_config.sl.yml"), domainDataOnly)
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

  def run(args: Array[String]): Try[Boolean] = Try {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    Xls2YmlConfig.parse(args) match {
      case Some(config) =>
        config.job match {
          case false =>
            config.files.foreach { file =>
              logger.info(s"Generating schemas for $file")
              writeDomainsAsYaml(file, config.outputPath)
            }
          case true =>
            config.files.foreach(
              Xls2YmlAutoJob.generateSchema(_, config.policyFile, config.outputPath)
            )
        }
        config.iamPolicyTagsFile.foreach { iamPolicyTagsPath =>
          val workbook = new XlsIamPolicyTagsReader(InputPath(iamPolicyTagsPath))
          val iamPolicyTags = IamPolicyTags(workbook.iamPolicyTags)
          writeIamPolicyTagsAsYaml(
            iamPolicyTags,
            config.outputPath.getOrElse(DatasetArea.metadata.toString),
            "iam-policy-tags"
          )
        }
        true
      case _ =>
        false
    }
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
