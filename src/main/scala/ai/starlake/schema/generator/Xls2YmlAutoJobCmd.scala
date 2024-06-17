package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult

import scala.util.{Success, Try}

object Xls2YmlAutoJobCmd extends Xls2YmlCmd {

  override def command: String = "xls2ymljob"

  override def run(config: Xls2YmlConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Xls2YmlAutoJob.generateSchema(config.files.head, config.policyFile, config.outputPath)
    Success(JobResult.empty)
  }
}
