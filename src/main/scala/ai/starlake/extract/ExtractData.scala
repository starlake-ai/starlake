package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Formatter._
import ai.starlake.utils.YamlSerializer
import better.files.File
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

object ExtractData extends Extract with LazyLogging {

  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    ExtractDataConfig.parse(args) match {
      case Some(config) =>
        run(config)
      case None =>
        throw new Exception(s"Could not parse arguments ${args.mkString(" ")}")
    }
  }

  /** Generate YML file from JDBC Schema stored in a YML file
    *
    * @param jdbcMapFile
    *   : Yaml File containing the JDBC Schema to extract
    * @param ymlOutputDir
    *   : Where to output the YML file. The generated filename will be in the for
    *   TABLE_SCHEMA_NAME.yml
    * @param settings
    *   : Application configuration file
    */
  def run(config: ExtractDataConfig)(implicit settings: Settings): Unit = {
    import settings.storageHandler
    val schemaHandler = new SchemaHandler(storageHandler)
    val content = settings.storageHandler
      .read(mappingPath(config.mapping))
      .richFormat(schemaHandler.activeEnv(), Map.empty)
    val jdbcSchemas =
      YamlSerializer.deserializeJDBCSchemas(content, config.mapping)
    val connectionOptions = jdbcSchemas.connectionRef
      .map(settings.comet.connections(_).options)
      .getOrElse(jdbcSchemas.connection)
    jdbcSchemas.jdbcSchemas.foreach { jdbcSchema =>
      extractData(
        jdbcSchema,
        connectionOptions,
        outputDir(config.outputDir),
        config.limit,
        config.separator
      )
    }
  }

  def extractData(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    outputDir: File,
    limit: Int,
    separator: String
  )(implicit
    settings: Settings
  ): Unit = {
    JDBCUtils.extractData(jdbcSchema, connectionOptions, outputDir, limit, separator)
  }

}
