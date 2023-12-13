package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.utils.{Utils, YamlSerializer}
import better.files.File
import com.typesafe.scalalogging.LazyLogging

import java.util.regex.Pattern
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class ExtractJDBCSchema(schemaHandler: SchemaHandler) extends Extract with LazyLogging {

  implicit val schemaHandlerImplicit: SchemaHandler = schemaHandler
  def run(args: Array[String])(implicit settings: Settings): Try[Unit] = {
    ExtractJDBCSchemaCmd.run(args, schemaHandler).map(_ => ())
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
    ExtractUtils.timeIt("Schema extraction") {
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
      implicit val forkJoinTaskSupport: Option[ForkJoinTaskSupport] =
        ExtractUtils.createForkSupport(config.parallelism)
      ExtractUtils.makeParallel(jdbcSchemas.jdbcSchemas).foreach { jdbcSchema =>
        val domainTemplate = jdbcSchema.template.map { ymlTemplate =>
          val content = settings
            .storageHandler()
            .read(mappingPath(ymlTemplate))
          YamlSerializer.deserializeDomain(content, ymlTemplate) match {
            case Success(domain) =>
              domain
            case Failure(e) => throw e
          }
        }
        val currentDomain = schemaHandler.getDomain(jdbcSchema.schema, raw = true)
        ExtractUtils.timeIt(s"Schema extraction of ${jdbcSchema.schema}") {
          extractSchema(
            jdbcSchema,
            connectionOptions,
            outputDir(config.outputDir),
            domainTemplate,
            currentDomain
          )
        }
      }
    }
  }

  def extractSchema(
    jdbcSchema: JDBCSchema,
    connectionOptions: Map[String, String],
    baseOutputDir: File,
    domainTemplate: Option[Domain],
    currentDomain: Option[Domain]
  )(implicit
    settings: Settings,
    fjp: Option[ForkJoinTaskSupport]
  ): Unit = {
    val domainName = jdbcSchema.sanitizeName match {
      case Some(true) => Utils.keepAlphaNum(jdbcSchema.schema)
      case _          => jdbcSchema.schema
    }
    baseOutputDir.createDirectories()
    File(baseOutputDir, domainName).createDirectories()
    val extractedDomain = extractDomain(jdbcSchema, connectionOptions, domainTemplate)
    val domain = extractedDomain.copy(
      comment = extractedDomain.comment.orElse(currentDomain.flatMap(_.comment)),
      tags =
        if (extractedDomain.tags.nonEmpty) extractedDomain.tags
        else currentDomain.map(_.tags).getOrElse(Set.empty),
      rename = extractedDomain.rename.orElse(currentDomain.flatMap(_.rename)),
      database = extractedDomain.database.orElse(currentDomain.flatMap(_.database)),
      metadata = Metadata
        .mergeAll(Nil ++ currentDomain.flatMap(_.metadata) ++ extractedDomain.metadata)
        .copy(fillWithDefaultValue = false)
        .asOption()
    )
    val tables = domain.tables
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
      val file = File(baseOutputDir, domainName, table.name + ".sl.yml")
      file.overwrite(content)
    }

    val finalDomain = domain.copy(tables = Nil)
    YamlSerializer.serializeToFile(
      File(baseOutputDir, domainName, "_config.sl.yml"),
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
    settings: Settings,
    fjp: Option[ForkJoinTaskSupport]
  ): Domain = {
    val selectedTablesAndColumns =
      JdbcDbUtils.extractJDBCTables(jdbcSchema, connectionOptions, skipRemarks = false)
    JdbcDbUtils.extractDomain(jdbcSchema, domainTemplate, selectedTablesAndColumns)
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
