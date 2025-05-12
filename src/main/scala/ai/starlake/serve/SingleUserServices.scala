package ai.starlake.serve

import ai.starlake.config.Settings
import ai.starlake.job.Main
import ai.starlake.schema.handlers.{DomainWithNameOnly, SchemaHandler}
import ai.starlake.schema.model.{AutoJobDesc, Domain, Type}

import scala.util.Try

object SingleUserServices {
  val core = new Main()
  var schemaHandler: SchemaHandler = null
  def getSchemaHandler(reload: Boolean)(implicit settings: Settings): SchemaHandler = {
    if (reload || schemaHandler == null) {
      schemaHandler = settings.schemaHandler()
    }
    schemaHandler
  }

  def reset(reload: Boolean)(implicit settings: Settings): String = {
    val result = SingleUserMainServer.mapper.writeValueAsString(CaffeineSettingsManager.reset())
    // external(reload)
    result
  }

  def domains(reload: Boolean)(implicit settings: Settings): List[Domain] = {
    getSchemaHandler(reload).domains()
  }

  def jobs(reload: Boolean)(implicit settings: Settings): List[AutoJobDesc] = {
    getSchemaHandler(reload).jobs()
  }

  def types(reload: Boolean)(implicit settings: Settings): List[Type] = {
    getSchemaHandler(reload).types()
  }

  def objectNames()(implicit settings: Settings): List[DomainWithNameOnly] = {
    getSchemaHandler(reload = false).objectNames()
  }

  def core(args: Array[String], reload: Boolean)(implicit settings: Settings): Try[Any] = {
    core.run(args, getSchemaHandler(reload))(settings)
  }

  def external(reload: Boolean)(implicit settings: Settings): Unit = {
    core(Array("extract-bq-schema", "--external"), reload)
  }

  def targetDatawarehHouse()(implicit settings: Settings): String = {
    settings.appConfig.getDefaultConnection().targetDatawareHouse()
  }
}
