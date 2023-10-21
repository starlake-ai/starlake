package ai.starlake.serve

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{DomainWithNameOnly, SchemaHandler}
import ai.starlake.schema.model.{AutoJobDesc, Domain, Type}

object SingleUserServices {

  def domains()(implicit settings: Settings): List[Domain] = {
    val schemaHandler = new SchemaHandler(settings.storageHandler())
    schemaHandler.domains(reload = true)
  }

  def jobs()(implicit settings: Settings): List[AutoJobDesc] = {
    val schemaHandler = new SchemaHandler(settings.storageHandler())
    schemaHandler.jobs()
  }

  def types()(implicit settings: Settings): List[Type] = {
    val schemaHandler = new SchemaHandler(settings.storageHandler())
    schemaHandler.types()
  }

  def objectNames()(implicit settings: Settings): List[DomainWithNameOnly] = {
    val schemaHandler = new SchemaHandler(settings.storageHandler())
    schemaHandler.getObjectNames()
  }
}
