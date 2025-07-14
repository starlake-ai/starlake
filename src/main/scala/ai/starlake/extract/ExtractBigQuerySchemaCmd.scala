package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{DomainInfo, JDBCSchema}
import ai.starlake.utils.JobResult

import scala.util.Try

object ExtractBigQuerySchemaCmd extends BigQueryTablesCmd {
  override def command: String = "extract-bq-schema"
  def fromExtractSchemaConfig(
    config: ExtractSchemaConfig,
    jdbcSchema: JDBCSchema
  ): BigQueryTablesConfig = {
    val tablesRenamed = jdbcSchema.tables.map { table =>
      if (table.name == "_" || table.name == "") "*" else table.name
    }
    val tables =
      if (jdbcSchema.schema.isEmpty)
        Map.empty[String, List[String]]
      else
        Map(jdbcSchema.schema -> tablesRenamed)
    BigQueryTablesConfig(
      tables = tables,
      database = jdbcSchema.catalog,
      external = config.external
    )
  }

  override def run(config: BigQueryTablesConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    Try {
      extract(config, schemaHandler)
    }.map(_ => JobResult.empty)

  def extract(config: BigQueryTablesConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): List[DomainInfo] = {
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
