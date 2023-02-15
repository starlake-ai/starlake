package ai.starlake.serve

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{AutoJobDesc, Domain, Type}

object Services {

  def domains()(implicit settings: Settings): List[Domain] = {
    val schemaHandler = new SchemaHandler(settings.storageHandler)
    schemaHandler.domains(true)
  }

  def jobs()(implicit settings: Settings): List[AutoJobDesc] = {
    val schemaHandler = new SchemaHandler(settings.storageHandler)
    schemaHandler.jobs(true).values.toList
  }

  def types()(implicit settings: Settings): List[Type] = {
    val schemaHandler = new SchemaHandler(settings.storageHandler)
    schemaHandler.types(true)
  }
}
