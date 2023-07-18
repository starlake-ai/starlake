package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{
  AttributeMergeStrategy,
  Domain,
  KeepOnlyScriptDiff,
  Metadata,
  RefFirst,
  SchemaRefs
}
import ai.starlake.utils.Formatter._
import ai.starlake.utils.YamlSerializer
import better.files.File
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import java.util.regex.Pattern
import scala.util.matching.Regex
import scala.util.{Failure, Success}

class ExtractJDBCSchema(schemaHandler: SchemaHandler) extends Extract with LazyLogging {

  implicit val schemaHandlerImplicit = schemaHandler
  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    ExtractSchemaConfig.parse(args) match {
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
    val content = settings
      .storageHandler()
      .read(mappingPath(config.extractConfig))
      .richFormat(schemaHandler.activeEnvVars(), Map.empty)
    val jdbcSchemas =
      YamlSerializer.deserializeJDBCSchemas(content, config.extractConfig)
    val connectionOptions = jdbcSchemas.connectionRef
      .map(settings.comet.connections(_).options)
      .getOrElse(jdbcSchemas.connection)
    jdbcSchemas.jdbcSchemas.foreach { jdbcSchema =>
      val domainTemplate = jdbcSchema.template.map { ymlTemplate =>
        val content = settings
          .storageHandler()
          .read(mappingPath(ymlTemplate))
        YamlSerializer.deserializeDomain(content, ymlTemplate) match {
          case Success(domain) =>
            if (domain.resolveDirectoryOpt().isEmpty)
              throw new Exception(
                "Domain metadata directory property is mandatory in template file."
              )
            domain
          case Failure(e) => throw e
        }
      }
      val currentDomain = schemaHandler.getDomain(jdbcSchema.schema, raw = true)
      extractSchema(
        jdbcSchema,
        connectionOptions,
        outputDir(config.outputDir),
        domainTemplate,
        currentDomain
      )
    }
  }

  def extractSchema(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    baseOutputDir: File,
    domainTemplate: Option[Domain],
    currentDomain: Option[Domain]
  )(implicit
    settings: Settings
  ): Unit = {
    val domainName = jdbcSchema.schema.replaceAll("[^\\p{Alnum}]", "_")
    baseOutputDir.createDirectories()
    File(baseOutputDir, domainName).createDirectories()
    val extractedDomain = extractDomain(jdbcSchema, connectionOptions, domainTemplate)
    val domain = extractedDomain.copy(metadata =
      Metadata
        .mergeAll(Nil ++ currentDomain.flatMap(_.metadata) ++ extractedDomain.metadata)
        .copy(fillWithDefaultValue = false)
        .asOption()
    )
    val tables = domain.tables
    val tableRefs = tables.map("_" + _.name)
    tables.foreach { table =>
      val restoredTable =
        currentDomain.flatMap(_.tables.find(_.name == table.name)) match {
          case Some(currentTable) =>
            val mergedTable = table.mergeWith(
              currentTable,
              domain.metadata,
              AttributeMergeStrategy(
                failOnContainerMismatch = false,
                failOnAttributesEmptinessMismatch = false,
                keepSourceDiffAttributesStrategy = KeepOnlyScriptDiff,
                attributePropertiesMergeStrategy = RefFirst
              )
            )
            mergedTable.copy(metadata =
              mergedTable.metadata.map(_.copy(fillWithDefaultValue = false))
            )
          case None =>
            table.copy(metadata =
              Metadata
                .mergeAll(Nil ++ domain.metadata ++ table.metadata)
                .`keepIfDifferent`(
                  domain.metadata.getOrElse(Metadata())
                )
                .copy(fillWithDefaultValue = false)
                .asOption()
            )
        }

      val tableWithPatternAndWrite = jdbcSchema.pattern match {
        case None => restoredTable
        case Some(pattern) =>
          val interpolatePattern = formatExtractPattern(jdbcSchema, table.name, pattern)
          val pat = Pattern.compile(interpolatePattern)
          restoredTable.copy(pattern = pat)
      }

      val content = YamlSerializer.serialize(SchemaRefs(List(tableWithPatternAndWrite)))
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
  private def extractDomain(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    domainTemplate: Option[Domain]
  )(implicit
    settings: Settings
  ): Domain = {
    val selectedTablesAndColumns =
      JDBCUtils.extractJDBCTables(jdbcSchema, connectionOptions, skipRemarks = false)
    JDBCUtils.extractDomain(jdbcSchema, domainTemplate, selectedTablesAndColumns)
  }

  private def formatExtractPattern(
    jdbcSchema: JDBCSchema,
    table: String,
    pattern: String
  )(implicit settings: Settings): String = {
    pattern.richFormat(
      Map(
        "catalog" -> jdbcSchema.catalog.map(Regex.quote).getOrElse(""),
        "schema"  -> Regex.quote(jdbcSchema.schema),
        "table"   -> Regex.quote(table)
      ),
      Map.empty
    )
  }
}
