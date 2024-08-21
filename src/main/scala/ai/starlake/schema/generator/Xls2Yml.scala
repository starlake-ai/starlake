package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerde._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import scala.util.Try

object Xls2Yml extends LazyLogging {

  def writeDomainsAsYaml(inputPath: String, outputPath: Option[String] = None)(implicit
    settings: Settings
  ): Unit = {
    val reader = new XlsDomainReader(InputPath(inputPath))
    reader.getDomain().foreach { domain =>
      writeDomainAsYaml(domain, new Path(outputPath.getOrElse(DatasetArea.load.toString)))(
        settings.storageHandler()
      )
    }
  }

  def writeDomainAsYaml(domain: Domain, basePath: Path)(implicit
    storageHandler: StorageHandler
  ): Unit = {
    logger.info(s"""Generated schemas:
         |${serialize(domain)}""".stripMargin)
    domain.writeDomainAsYaml(basePath)
  }

  def writeIamPolicyTagsAsYaml(
    iamPolicyTags: IamPolicyTags,
    outputPath: String,
    fileName: String
  )(implicit storageHandler: StorageHandler): Unit = {
    logger.info(s"""Generated schemas:
         |${serialize(iamPolicyTags)}""".stripMargin)
    serializeToPath(new Path(outputPath, s"${fileName}.sl.yml"), iamPolicyTags)
  }

  def run(args: Array[String]): Try[Boolean] = {
    implicit val settings: Settings = Settings(Settings.referenceConfig, None, None)
    Xls2YmlCmd.run(args.toIndexedSeq, new SchemaHandler(settings.storageHandler())).map(_ => true)
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
