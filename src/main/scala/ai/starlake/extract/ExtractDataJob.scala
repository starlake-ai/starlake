package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Formatter._
import ai.starlake.utils.YamlSerializer
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

class ExtractDataJob(schemaHandler: SchemaHandler) extends Extract with LazyLogging {

  def run(args: Array[String])(implicit settings: Settings): Try[Unit] = {
    ExtractDataCmd.run(args, schemaHandler).map(_ => ())
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
    config: UserExtractDataConfig
  )(implicit settings: Settings): Unit = {
    val content = settings
      .storageHandler()
      .read(mappingPath(config.extractConfig))
      .richFormat(schemaHandler.activeEnvVars(), Map.empty)
    val jdbcSchemas =
      YamlSerializer.deserializeJDBCSchemas(content, config.extractConfig)
    val dataConnectionSettings = jdbcSchemas.connectionRef match {
      case Some(connectionRef) => settings.appConfig.getConnection(connectionRef)
      case None                => throw new Exception(s"No connectionRef defined for jdbc schemas.")
    }
    val auditConnectionRef =
      jdbcSchemas.auditConnectionRef.getOrElse(settings.appConfig.audit.getConnectionRef())

    val auditConnectionSettings = settings.appConfig.getConnection(auditConnectionRef)
    val fileFormat = jdbcSchemas.output.getOrElse(FileFormat()).fillWithDefault()
    logger.info(s"Extraction will be formatted following $fileFormat")

    implicit val implicitSchemaHandler: SchemaHandler = schemaHandler
    jdbcSchemas.jdbcSchemas
      .filter { s =>
        (config.includeSchemas, config.excludeSchemas) match {
          case (Nil, Nil) => true
          case (inc, Nil) => inc.map(_.toLowerCase).contains(s.schema.toLowerCase)
          case (Nil, exc) => !exc.map(_.toLowerCase).contains(s.schema.toLowerCase)
          case (_, _) =>
            throw new RuntimeException(
              "You can't specify includeShemas and excludeSchemas at the same time"
            )
        }
      }
      .foreach { jdbcSchema =>
        assert(config.numPartitions > 0)
        JdbcDbUtils.extractData(
          ExtractDataConfig(
            jdbcSchema,
            dataOutputDir(config.outputDir),
            config.limit,
            config.numPartitions,
            config.parallelism,
            config.fullExport,
            config.ifExtractedBefore
              .map(userTimestamp => lastTimestamp => lastTimestamp < userTimestamp),
            config.ignoreExtractionFailure,
            config.cleanOnExtract,
            config.includeTables,
            config.excludeTables,
            fileFormat,
            dataConnectionSettings.mergeOptionsWith(jdbcSchema.connectionOptions),
            auditConnectionSettings
          )
        )
      }
  }
}
