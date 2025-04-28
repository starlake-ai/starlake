package ai.starlake.extract

import ai.starlake.config.Settings.Connection
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.core.utils.StringUtils
import ai.starlake.extract.spi.SchemaExtractorWorkflow
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.utils.YamlSerde
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.io.FileNotFoundException
import java.util.regex.Pattern
import scala.annotation.nowarn
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class ExtractSchema(schemaHandler: SchemaHandler) extends ExtractPathHelper with LazyLogging {

  implicit val schemaHandlerImplicit: SchemaHandler = schemaHandler

  @nowarn
  def run(args: Array[String])(implicit settings: Settings): Try[Unit] = {
    ExtractSchemaCmd.run(args, schemaHandler).map(_ => ())
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
  def run(config: ExtractSchemaConfig, jdbcSchemas: ExtractSchemas)(implicit settings: Settings) = {
    val connectionSettings: Connection = jdbcSchemas.connectionRef match {
      case Some(connectionRef) =>
        settings.appConfig.getConnection(connectionRef).withAccessToken(config.accessToken)
      case None =>
        config.connectionRef
          .map(settings.appConfig.getConnection)
          .getOrElse(settings.appConfig.getDefaultConnection())
          .withAccessToken(config.accessToken)
    }
    jdbcSchemas.openAPI match {
      case Some(openAPIConfig) =>
        // TODO: temporarily doing switch case but plans to remove it once all others extractions implement Schema Extractor
        // TODO: implement templates as in jdbcSchema
        SchemaExtractorWorkflow.run(config, jdbcSchemas)
      case None =>
        val extractArea = if (config.external) DatasetArea.external else DatasetArea.extract
        if (connectionSettings.isBigQuery()) {
          for (jdbcSchema <- jdbcSchemas.jdbcSchemas.getOrElse(Nil)) {
            val bigQueryConfig =
              ExtractBigQuerySchemaCmd.fromExtractSchemaConfig(config, jdbcSchema)
            ExtractBigQuerySchemaCmd.run(bigQueryConfig, schemaHandler)
          }
        } else { // JDBC
          implicit val forkJoinTaskSupport: Option[ForkJoinTaskSupport] =
            ParUtils.createForkSupport(config.parallelism)
          ParUtils.makeParallel(jdbcSchemas.jdbcSchemas.getOrElse(Nil)).iterator.foreach {
            jdbcSchema =>
              val domainTemplate = jdbcSchema.template
                .orElse {
                  val defaultDomainFile = "_domain_" + config.extractConfig
                  if (
                    settings.storageHandler().exists(mappingPath(extractArea, defaultDomainFile))
                  ) {
                    Some(defaultDomainFile)
                  } else {
                    None
                  }
                }
                .map { ymlTemplate =>
                  val domainTemplatePath = mappingPath(extractArea, ymlTemplate)
                  logger.info(s"Loading domain template from $domainTemplatePath")
                  val content = settings
                    .storageHandler()
                    .read(domainTemplatePath)
                  YamlSerde
                    .deserializeYamlLoadConfig(content, ymlTemplate, isForExtract = true) match {
                    case Success(domain) =>
                      val tableTemplatePath = mappingPath(
                        extractArea,
                        jdbcSchema.template
                          .map(t => "_table_" + t)
                          .getOrElse("_table_" + config.extractConfig)
                      )
                      if (settings.storageHandler().exists(tableTemplatePath)) {
                        logger.info(s"Loading table template from $tableTemplatePath")
                        val tableContent = settings
                          .storageHandler()
                          .read(tableTemplatePath)
                        val tableDesc: List[TableDesc] =
                          YamlSerde.deserializeYamlTables(tableContent, tableTemplatePath.toString)
                        domain.copy(tables = tableDesc.map(_.table))
                      } else {
                        domain
                      }
                    case Failure(e) => throw e
                  }
                }
              val currentDomain = schemaHandler.getDomain(jdbcSchema.schema, raw = true)
              ExtractUtils.timeIt(s"Schema extraction of ${jdbcSchema.schema}") {
                extractSchema(
                  jdbcSchema,
                  connectionSettings,
                  schemaOutputDir(config),
                  domainTemplate,
                  currentDomain,
                  config.external
                )
              }
          }
          forkJoinTaskSupport.foreach(_.forkJoinPool.shutdown())
        }
    }
  }

  def run(config: ExtractSchemaConfig)(implicit settings: Settings): Unit = {
    ExtractUtils.timeIt("Schema extraction") {
      val extractSchemasConfig = fromConfig(config)
      run(config, extractSchemasConfig)
    }
  }

  private def fromConfig(
    config: ExtractSchemaConfig
  )(implicit settings: _root_.ai.starlake.config.Settings): ExtractSchemas = {
    val userConfig =
      Try {
        val extractArea = if (config.external) DatasetArea.external else DatasetArea.extract
        val extractConfigPath = mappingPath(extractArea, config.extractConfig)
        settings.storageHandler().exists(extractConfigPath) match {
          case false =>
            throw new FileNotFoundException(
              s"Could not found extract config ${config.extractConfig}. Please check its existence."
            )
          case true => // do nothing
            val content = settings
              .storageHandler()
              .read(extractConfigPath)
              .richFormat(schemaHandler.activeEnvVars(), Map.empty)
            val extractSchemasConfig =
              YamlSerde.deserializeYamlExtractConfig(content, config.extractConfig)
            extractSchemasConfig
        }
      }
    if (config.all) {
      val connectionRef =
        userConfig.toOption
          .flatMap(_.connectionRef)
          .orElse(config.connectionRef)
          .getOrElse(settings.appConfig.connectionRef)
      val schemaNames = SchemaExtractor.extractSchemaNames(connectionRef, config.accessToken)
      val result =
        schemaNames match {
          case Failure(e) =>
            throw e
          case Success(schemaNames) =>
            val jdbcSchemas = schemaNames.map { case (schema, tables) =>
              val jdbcSchema = JDBCSchema(
                schema = schema,
                tables = tables.map { t => new JDBCTable().copy(name = t) },
                pattern = None,
                template = None
              )
              jdbcSchema
            }
            ExtractSchemas(jdbcSchemas = Some(jdbcSchemas), connectionRef = Some(connectionRef))
        }
      result
    } else {
      if (config.tables.isEmpty && config.extractConfig.isEmpty) {
        throw new Exception("Either tables or extractConfig must be defined")
      }
      if (config.tables.nonEmpty) {
        val jdbcTablesDesc = config.tables
          .map { table =>
            val parts = table.split("\\.")
            if (parts.length != 2) {
              throw new Exception(s"Invalid table format: $table")
            }
            val schema = parts(0)
            val t = parts(1)
            (schema, t)
          }
          .groupBy(_._1)
          .toList

        val jdbcSchemas =
          jdbcTablesDesc.map { case (schema, tables) =>
            val jdbcSchema = JDBCSchema(
              schema = schema,
              tables = tables.map { t => new JDBCTable().copy(name = t._2) }.toList,
              pattern = None,
              template = None
            )
            jdbcSchema
          }
        ExtractSchemas(jdbcSchemas = Some(jdbcSchemas), connectionRef = config.connectionRef)
      } else {
        userConfig match {
          case Success(extractSchemasConfig) =>
            extractSchemasConfig
          case Failure(e) =>
            throw e
        }
      }
    }
  }

  def extractSchema(
    jdbcSchema: JDBCSchema,
    connectionSettings: Connection,
    baseOutputDir: Path,
    domainTemplate: Option[Domain],
    currentDomain: Option[Domain],
    external: Boolean
  )(implicit
    settings: Settings,
    fjp: Option[ForkJoinTaskSupport]
  ): Unit = {
    implicit val storageHandler: StorageHandler = settings.storageHandler()
    val domainName = jdbcSchema.sanitizeName match {
      case Some(true) => StringUtils.replaceNonAlphanumericWithUnderscore(jdbcSchema.schema)
      case _          => jdbcSchema.schema
    }
    storageHandler.mkdirs(new Path(baseOutputDir, domainName))
    val extractedDomain = extractDomain(jdbcSchema, connectionSettings, domainTemplate)
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
    val tables =
      domain.tables.map { table =>
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
        tableWithPatternAndWrite
      }

    val finalDomain = domain.copy(tables = tables)

    if (external) {
      schemaHandler.saveToExternals(List(finalDomain))
    } else {
      schemaHandler.saveTo(List(finalDomain), baseOutputDir)
    }
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
    connectionSettings: Connection,
    domainTemplate: Option[Domain]
  )(implicit
    settings: Settings,
    fjp: Option[ForkJoinTaskSupport]
  ): Domain = {
    val selectedTablesAndColumns =
      JdbcDbUtils.extractJDBCTables(
        jdbcSchema,
        connectionSettings,
        skipRemarks = false,
        keepOriginalName = false,
        includeColumns = true
      )
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
