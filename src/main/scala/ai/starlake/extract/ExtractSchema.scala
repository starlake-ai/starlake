package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Domain, Metadata, Schemas}
import ai.starlake.utils.Formatter._
import ai.starlake.utils.YamlSerializer
import better.files.File
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern
import scala.util.{Failure, Success}

object ExtractSchema extends Extract with LazyLogging {

  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    ExtractSchemaConfig.parse(args.toSeq) match {
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
  def run(config: ExtractSchemaConfig)(implicit settings: Settings): Unit = {
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
      val domainTemplate = jdbcSchema.template.map { ymlTemplate =>
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
              case None =>
                throw new Exception(
                  "Domain metadata directory property is mandatory in template file."
                )
            }
            domain
          case Failure(e) => throw e
        }
      }
      extractSchema(jdbcSchema, connectionOptions, outputDir(config.outputDir), domainTemplate)
    }
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
    baseOutputDir.createDirectories()
    File(baseOutputDir, domainName).createDirectories()
    val domain = extractDomain(jdbcSchema, connectionOptions, domainTemplate)
    val tables = domain.tables
    val tableRefs = tables.map("_" + _.name)
    tables.foreach { table =>
      val tableWithWrite = jdbcSchema.write match {
        case None => table
        case Some(write) =>
          val metadata =
            domainTemplate.flatMap(_.metadata).getOrElse(Metadata()).copy(write = jdbcSchema.write)
          table.copy(metadata = Some(metadata))
      }
      val tableWithPatternAndWrite = jdbcSchema.pattern match {
        case None => tableWithWrite
        case Some(pattern) =>
          val pat = Pattern.compile(pattern)
          tableWithWrite.copy(pattern = pat)
      }

      val content = YamlSerializer.serialize(Schemas(List(tableWithPatternAndWrite)))
      val file = File(baseOutputDir, domainName, "_" + table.name + ".comet.yml")
      file.overwrite(content)
    }

    val finalDomain = domain.copy(tableRefs = tableRefs, tables = Nil)
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
}
