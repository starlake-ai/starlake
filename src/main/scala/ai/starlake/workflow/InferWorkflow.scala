package ai.starlake.workflow

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.infer.{InferSchemaConfig, InferSchemaJob}
import ai.starlake.schema.generator.{Yml2DDLConfig, Yml2DDLJob}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Utils
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import scala.util.Try

trait InferWorkflow extends LazyLogging {
  this: IngestionWorkflow =>

  protected def schemaHandler: SchemaHandler
  implicit protected def settings: Settings

  def inferSchema(config: InferSchemaConfig): Try[Path] = {
    val domainName =
      if (config.domainName.isEmpty) {
        val file = File(config.inputPath)
        file.parent.name
      } else {
        config.domainName
      }

    val saveDir = config.outputDir.getOrElse(DatasetArea.load.toString)

    val (name, write) = config.extractTableNameAndWriteMode()
    val tableName =
      if (config.schemaName.isEmpty)
        name
      else
        config.schemaName
    val result = (new InferSchemaJob).infer(
      domainName = domainName,
      tableName = tableName,
      pattern = None,
      comment = None,
      inputPath = config.inputPath,
      saveDir = if (saveDir.isEmpty) DatasetArea.load.toString else saveDir,
      forceFormat = config.format,
      writeMode = config.write.getOrElse(write),
      rowTag = config.rowTag,
      clean = config.clean,
      encoding = config.encoding,
      variant = config.variant.getOrElse(false),
      fromJsonSchema = config.fromJsonSchema
    )(settings.storageHandler())
    Utils.logFailure(result, logger)
    result
  }

  def inferDDL(config: Yml2DDLConfig): Try[Unit] = {
    val result = new Yml2DDLJob(config, schemaHandler).run()
    Utils.logFailure(result, logger)
  }
}