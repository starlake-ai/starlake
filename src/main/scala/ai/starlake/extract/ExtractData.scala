package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Formatter._
import ai.starlake.utils.YamlSerializer
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

class ExtractData(schemaHandler: SchemaHandler) extends Extract with LazyLogging {

  def run(args: Array[String])(implicit settings: Settings): Try[Unit] = Try {
    ExtractDataConfig.parse(args) match {
      case Some(config) =>
        run(config)
      case None =>
        throw new IllegalArgumentException(ExtractDataConfig.usage())
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
  def run(
    config: ExtractDataConfig
  )(implicit settings: Settings): Unit = {
    val content = settings
      .storageHandler()
      .read(mappingPath(config.extractConfig))
      .richFormat(schemaHandler.activeEnvVars(), Map.empty)
    val jdbcSchemas =
      YamlSerializer.deserializeJDBCSchemas(content, config.extractConfig)
    val connectionOptions = jdbcSchemas.connectionRef
      .map(settings.appConfig.connections(_).options)
      .getOrElse(
        throw new Exception(s"No connectionRef found. Please check your connectionRef property")
      )
    val fileFormat = jdbcSchemas.output.getOrElse(FileFormat()).fillWithDefault()
    logger.info(s"Extraction will be formatted following $fileFormat")

    jdbcSchemas.jdbcSchemas
      .filter { s =>
        (config.includeSchemas, config.excludeSchemas) match {
          case (Nil, Nil) => true
          case (inc, Nil) => inc.map(_.toLowerCase).contains(s.schema.toLowerCase)
          case (Nil, exc) => !exc.map(_.toLowerCase).contains(s.schema.toLowerCase)
          case (_, _)     => throw new RuntimeException("Should not happen")
        }
      }
      .foreach { jdbcSchema =>
        assert(config.numPartitions > 0)
        JdbcDbUtils.extractData(
          schemaHandler,
          jdbcSchema,
          connectionOptions ++ jdbcSchema.connectionOptions,
          outputDir(config.outputDir),
          config.limit,
          config.numPartitions,
          config.parallelism,
          config.fullExport,
          config.ifExtractedBefore
            .map(userTimestamp => lastTimestamp => lastTimestamp < userTimestamp),
          config.cleanOnExtract,
          config.includeTables,
          config.excludeTables,
          fileFormat
        )
      }
  }
}
