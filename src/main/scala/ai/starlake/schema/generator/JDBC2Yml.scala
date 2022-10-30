package ai.starlake.schema.generator

import better.files.File
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Domain, Schemas}
import ai.starlake.utils.YamlSerializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success}
import ai.starlake.utils.Formatter._

object JDBC2Yml extends LazyLogging {

  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    JDBC2YmlConfig.parse(args) match {
      case Some(config) =>
        run(config)
      case None =>
        throw new Exception(s"Could not parse arguments ${args.mkString(" ")}")
    }
  }

  private def mappingPath(config: JDBC2YmlConfig)(implicit settings: Settings): Path = {
    val mappingFilename =
      if (config.mapping.endsWith(".comet.yml")) config.mapping else config.mapping + ".comet.yml"
    val paths =
      settings.storageHandler.list(DatasetArea.extract, extension = ".yml", recursive = false)

    paths.find(_.getName() == mappingFilename).getOrElse(new Path(mappingFilename))

  }

  private def outputDir(config: JDBC2YmlConfig)(implicit settings: Settings): File =
    File(config.outputDir.getOrElse(DatasetArea.extract.toString))

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
  def run(config: JDBC2YmlConfig)(implicit settings: Settings): Unit = {
    import settings.{metadataStorageHandler}
    val schemaHandler = new SchemaHandler(metadataStorageHandler)
    val content = settings.storageHandler
      .read(mappingPath(config))
      .richFormat(schemaHandler.activeEnv(), Map.empty)
    val jdbcSchemas =
      YamlSerializer.deserializeJDBCSchemas(content, config.mapping)
    config.mode match {
      case "data" =>
        jdbcSchemas.jdbcSchemas.foreach { jdbcSchema =>
          val connectionOptions = jdbcSchema.connection
            .map(settings.comet.connections(_).options)
            .getOrElse(jdbcSchemas.connection)
          extractData(
            jdbcSchema,
            connectionOptions,
            outputDir(config),
            config.limit,
            config.separator
          )
        }
      case "schema" =>
        jdbcSchemas.jdbcSchemas.foreach { jdbcSchema =>
          val connectionOptions = jdbcSchema.connection
            .map(settings.comet.connections(_).options)
            .getOrElse(jdbcSchemas.connection)
          val domainTemplate = config.ymlTemplate.orElse(jdbcSchema.template).map { ymlTemplate =>
            val content = settings.storageHandler
              .read(new Path(ymlTemplate))
              .richFormat(schemaHandler.activeEnv(), Map.empty)
            YamlSerializer.deserializeDomain(content, ymlTemplate) match {
              case Success(domain) =>
                domain.metadata match {
                  case Some(metadata) if metadata.directory.isEmpty =>
                    throw new Exception(
                      "Domain metadata directory property is mandatory in template file."
                    )
                  case Some(_) =>
                    domain
                }
                domain
              case Failure(e) => throw e
            }
          }
          extractSchema(jdbcSchema, connectionOptions, outputDir(config), domainTemplate)
        }
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

  def extractSchema(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    baseOutputDir: File,
    domainTemplate: Option[Domain]
  )(implicit
    settings: Settings
  ): Unit = {

    val domainName = jdbcSchema.schema.replaceAll("[^\\p{Alnum}]", "_")
    File(baseOutputDir, domainName).createDirectories()
    val domain = extractDomain(jdbcSchema, connectionOptions, domainTemplate)
    val tables = domain.tables
    val tableRefs = tables.map("_" + _.name)
    tables.foreach { table =>
      val content = YamlSerializer.serialize(Schemas(List(table)))
      val file = File(baseOutputDir, domainName, "_" + table.name + ".comet.yml")
      file.overwrite(content)
    }
    val finalDomain = domain.copy(tableRefs = Some(tableRefs), tables = Nil)
    YamlSerializer.serializeToFile(
      File(baseOutputDir, domainName, domainName + ".comet.yml"),
      finalDomain
    )
  }

  /** Generate YML file from the JDBCSchema
    *
    * @param jdbcSchema
    *   : the JDBC Schema to extract
    * @param settings
    *   : Application configuration file
    */
  def extractDomain(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    domainTemplate: Option[Domain]
  )(implicit
    settings: Settings
  ): Domain = {
    val selectedTablesAndColumns = JDBCUtils.extractJDBCTables(jdbcSchema, connectionOptions)
    JDBCUtils.extractDomain(jdbcSchema, domainTemplate, selectedTablesAndColumns)
  }

  def main(args: Array[String]): Unit = {
    JDBC2Yml.run(args)
  }
}
