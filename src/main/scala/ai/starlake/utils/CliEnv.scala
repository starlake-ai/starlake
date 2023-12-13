package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.workflow.IngestionWorkflow

trait CliEnv {

  def options: Map[String, String]

  def schemaHandler(implicit settings: Settings): SchemaHandler = {
    val schemaHandler = new SchemaHandler(settings.storageHandler(), options)
    if (settings.appConfig.validateOnLoad)
      schemaHandler.checkValidity()
    schemaHandler
  }

  def workflow(implicit settings: Settings): IngestionWorkflow = {
    new IngestionWorkflow(settings.storageHandler(), schemaHandler)
  }
}
