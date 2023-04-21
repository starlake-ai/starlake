package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Formatter._
import ai.starlake.utils.YamlSerializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

class ExtractData(schemaHandler: SchemaHandler) extends Extract with LazyLogging {

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
    val schemaHandler = new SchemaHandler(settings.storageHandler)
    val content = settings.storageHandler
      .read(mappingPath(config.mapping))
      .richFormat(schemaHandler.activeEnv(), Map.empty)
    val jdbcSchemas =
      YamlSerializer.deserializeJDBCSchemas(content, config.mapping)
    val connectionOptions = jdbcSchemas.connectionRef
      .map(settings.comet.connections(_).options)
      .getOrElse(jdbcSchemas.connection)
    jdbcSchemas.jdbcSchemas
      .filter { s =>
        (config.includeSchemas, config.excludeSchemas) match {
          case (Nil, Nil) => true
          case (inc, Nil) => inc.contains(s.schema)
          case (Nil, exc) => !exc.contains(s.schema)
          case (_, _)     => throw new RuntimeException("Should not happen")
        }
      }
      .foreach { jdbcSchema =>
        assert(config.numPartitions > 0)
        JDBCUtils.extractData(
          schemaHandler,
          jdbcSchema,
          connectionOptions ++ jdbcSchema.connectionOptions,
          outputDir(config.outputDir),
          config.limit,
          config.separator,
          config.numPartitions,
          config.clean,
          config.parallelism.getOrElse(Runtime.getRuntime.availableProcessors()),
          config.fullExport,
          config.datePattern,
          config.timestampPattern,
          config.ifExtractedBefore
            .map(userTimestamp => lastTimestamp => lastTimestamp < userTimestamp),
          config.cleanOnExtract,
          config.includeTables,
          config.excludeTables
        )
      }
  }
}
