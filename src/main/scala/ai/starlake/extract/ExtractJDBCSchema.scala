package ai.starlake.extract

import ai.starlake.config.Settings.{latestSchemaVersion, Connection}
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.generator.ExtractBigQuerySchemaCmd
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.utils.{Utils, YamlSerde}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.io.FileNotFoundException
import java.util.regex.Pattern
import scala.annotation.nowarn
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class ExtractJDBCSchema(schemaHandler: SchemaHandler) extends Extract with LazyLogging {

  implicit val schemaHandlerImplicit: SchemaHandler = schemaHandler
  @nowarn
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
      val jdbcSchemas = fromConfig(config)
      val connectionSettings = jdbcSchemas.connectionRef match {
        case Some(connectionRef) => settings.appConfig.getConnection(connectionRef)
        case None => throw new Exception(s"No connectionRef defined for jdbc schemas.")
      }
      val extractArea = if (config.external) DatasetArea.external else DatasetArea.extract
      if (connectionSettings.isBigQuery()) {
        for (jdbcSchema <- jdbcSchemas.jdbcSchemas) {
          val bigQueryConfig = ExtractBigQuerySchemaCmd.fromExtractSchemaConfig(config, jdbcSchema)
          ExtractBigQuerySchemaCmd.run(bigQueryConfig, schemaHandler)
        }
      } else {
        implicit val forkJoinTaskSupport: Option[ForkJoinTaskSupport] =
          ParUtils.createForkSupport(config.parallelism)
        ParUtils.makeParallel(jdbcSchemas.jdbcSchemas).foreach { jdbcSchema =>
          val domainTemplate = jdbcSchema.template
            .orElse {
              val defaultDomainFile = "_domain_" + config.extractConfig
              if (settings.storageHandler().exists(mappingPath(extractArea, defaultDomainFile))) {
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
              YamlSerde.deserializeYamlLoadConfig(content, ymlTemplate, isForExtract = true) match {
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
                    val tableDesc: TablesDesc =
                      YamlSerde.deserializeYamlTables(tableContent, tableTemplatePath.toString)
                    domain.copy(tables = tableDesc.tables)
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
              schemaOutputDir(config.outputDir),
              domainTemplate,
              currentDomain
            )
          }
        }
        forkJoinTaskSupport.foreach(_.forkJoinPool.shutdown())
      }
    }
  }

  private def fromConfig(
    config: ExtractSchemaConfig
  )(implicit settings: _root_.ai.starlake.config.Settings): JDBCSchemas = {
    val extractArea = if (config.external) DatasetArea.external else DatasetArea.extract
    val extractConfigPath = mappingPath(extractArea, config.extractConfig)
    Try(settings.storageHandler().exists(extractConfigPath)) match {
      case Failure(_) | Success(false) =>
        throw new FileNotFoundException(
          s"Could not found extract config ${config.extractConfig}. Please check its existence."
        )
      case _ => // do nothing
    }
    val content = settings
      .storageHandler()
      .read(extractConfigPath)
      .richFormat(schemaHandler.activeEnvVars(), Map.empty)
    val jdbcSchemas =
      YamlSerde.deserializeYamlExtractConfig(content, config.extractConfig)
    jdbcSchemas
  }

  def extractSchema(
    jdbcSchema: JDBCSchema,
    connectionSettings: Connection,
    baseOutputDir: Path,
    domainTemplate: Option[Domain],
    currentDomain: Option[Domain]
  )(implicit
    settings: Settings,
    fjp: Option[ForkJoinTaskSupport]
  ): Unit = {
    implicit val storageHandler: StorageHandler = settings.storageHandler()
    val domainName = jdbcSchema.sanitizeName match {
      case Some(true) => Utils.keepAlphaNum(jdbcSchema.schema)
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

      val content =
        YamlSerde.serialize(TablesDesc(latestSchemaVersion, List(tableWithPatternAndWrite)))
      val file =
        new Path(new Path(baseOutputDir, domainName), table.name + ".sl.yml")
      storageHandler.delete(file)
      storageHandler.write(content, file)
    }

    val finalDomain = domain.copy(tables = Nil)
    YamlSerde.serializeToPath(
      new Path(new Path(baseOutputDir, domainName), "_config.sl.yml"),
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
        keepOriginalName = false
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
