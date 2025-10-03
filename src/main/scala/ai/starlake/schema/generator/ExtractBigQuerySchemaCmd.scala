package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.{BigQueryTablesCmd, ExtractSchemaConfig, JDBCSchema, TablesExtractConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.Domain
import ai.starlake.utils.JobResult

import scala.util.Try

object ExtractBigQuerySchemaCmd extends BigQueryTablesCmd {
  override def command: String = "extract-bq-schema"
  def fromExtractSchemaConfig(
    config: ExtractSchemaConfig,
    jdbcSchema: JDBCSchema
  ): TablesExtractConfig = {
    val tablesRenamed = jdbcSchema.tables.map { table =>
      if (table.name == "_" || table.name == "") "*" else table.name
    }
    val tables =
      if (jdbcSchema.schema.isEmpty)
        Map.empty[String, List[String]]
      else
        Map(jdbcSchema.schema -> tablesRenamed)
    TablesExtractConfig(
      tables = tables,
      database = jdbcSchema.catalog,
      external = config.external
    )
  }

  override def run(config: TablesExtractConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Try(if (config.external) {
      val externalSources = schemaHandler.externalSources()
      val externalDomains =
        ExtractBigQuerySchema.extractExternalDatasets(externalSources)(settings, schemaHandler)
      externalDomains.foreach { case (_, domains) =>
        domains.foreach { domain =>
          domain.writeDomainAsYaml(DatasetArea.external)(settings.storageHandler())
        }
      }
    } else {
      ExtractBigQuerySchema.extractAndSaveAsDomains(config)
    }).map(_ => JobResult.empty)
  }

  def extract(config: TablesExtractConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): List[Domain] = {
    if (config.external) {
      val extractor = new ExtractBigQuerySchema(config)
      val externalDomains = extractor.extractSchemasAndTables(schemaHandler, config.tables)
      schemaHandler.saveToExternals(externalDomains)
      externalDomains
    } else {
      ExtractBigQuerySchema.extractAndSaveToExternal(config, schemaHandler)
    }
  }
}
