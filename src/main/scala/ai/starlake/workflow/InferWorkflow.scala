package ai.starlake.workflow

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.infer.{InferSchemaConfig, InferSchemaJob}
import ai.starlake.schema.generator.{Yml2DDLConfig, Yml2DDLJob}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Utils
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import scala.util.{Success, Try}

/** Outcome of `InferWorkflow.inferSchema`: the resulting table YAML path, the domain/table it
  * belongs to, and whether inference actually ran or was skipped because a definition for that
  * domain/table already existed (and `clean` was not requested).
  */
final case class InferSchemaResult(
  tablePath: Path,
  domainName: String,
  tableName: String,
  skipped: Boolean
)

trait InferWorkflow extends LazyLogging {
  this: IngestionWorkflow =>

  protected def schemaHandler: SchemaHandler
  implicit protected def settings: Settings

  def inferSchema(config: InferSchemaConfig): Try[InferSchemaResult] = {
    val domainName =
      if (config.domainName.isEmpty) {
        val file = File(config.inputPath)
        file.parent.name
      } else {
        config.domainName
      }

    val saveDirOpt = config.outputDir.getOrElse(DatasetArea.load.toString)
    val saveDir = if (saveDirOpt.isEmpty) DatasetArea.load.toString else saveDirOpt

    val (name, write) = config.extractTableNameAndWriteMode()
    val tableName =
      if (config.schemaName.isEmpty)
        name
      else
        config.schemaName

    val tablePath = new Path(new Path(saveDir, domainName), s"$tableName.sl.yml")
    if (!config.clean && settings.storageHandler().exists(tablePath)) {
      // Table already has a schema definition: skip re-inference instead of failing.
      // This makes `autoload` idempotent on projects shipping predefined table definitions.
      // Pass --clean (infer-schema) / --clean (autoload) to force re-inference and overwrite it.
      logger.info(
        s"Table $tableName already defined in domain $domainName at $tablePath. Skipping inference."
      )
      Success(InferSchemaResult(tablePath, domainName, tableName, skipped = true))
    } else {
      val result = (new InferSchemaJob).infer(
        domainName = domainName,
        tableName = tableName,
        pattern = None,
        comment = None,
        inputPath = config.inputPath,
        saveDir = saveDir,
        forceFormat = config.format,
        writeMode = config.write.getOrElse(write),
        rowTag = config.rowTag,
        clean = config.clean,
        encoding = config.encoding,
        variant = config.variant.getOrElse(false),
        fromJsonSchema = config.fromJsonSchema
      )(settings.storageHandler())
      Utils.logFailure(result, logger)
      result.map(InferSchemaResult(_, domainName, tableName, skipped = false))
    }
  }

  def inferDDL(config: Yml2DDLConfig): Try[Unit] = {
    val result = new Yml2DDLJob(config, schemaHandler).run()
    Utils.logFailure(result, logger)
  }
}
