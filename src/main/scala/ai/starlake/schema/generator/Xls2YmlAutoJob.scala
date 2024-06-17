package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerde._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

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
    val reader = new XlsAutoJobReader(
      InputPath(inputPath),
      policyPath.map(InputPath),
      settings.storageHandler()
    )
    reader.autoTasksDesc
      .foreach { autoTask =>
        val taskPath = new Path(basePath, autoTask.domain)
        logger.info(s"Generating autoJob schema for ${autoTask.name} in $taskPath")
        writeAutoTaskYaml(
          autoTask,
          taskPath,
          autoTask.name
        )(settings.storageHandler())
      }
  }

  def writeAutoTaskYaml(autoTask: AutoTaskDesc, outputPath: Path, fileName: String)(implicit
    storageHandler: StorageHandler
  ): Unit = {
    storageHandler.mkdirs(outputPath)
    logger.info(s"""Generated autoJob schemas:
                   |${serialize(autoTask)}""".stripMargin)
    serializeToPath(new Path(outputPath, s"$fileName.sl.yml"), autoTask)
  }

  def run(args: Array[String]): Try[Unit] = {
    implicit val settings: Settings = Settings(Settings.referenceConfig)
    Xls2YmlAutoJobCmd
      .run(args.toIndexedSeq, new SchemaHandler(settings.storageHandler()))
      .map(_ => ())
  }

  def main(args: Array[String]): Unit = {
    Xls2YmlAutoJob.run(args)
    System.exit(0)
  }
}
