package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.{
  BigQueryTablesCmd,
  BigQueryTablesConfig,
  ExtractSchemaConfig,
  JDBCSchema
}
import ai.starlake.schema.handlers.SchemaHandler
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
      if (config.external) {
        val extractor = new ExtractBigQuerySchema(config)
        val externalDomains = extractor.extractDatasets(schemaHandler)
        externalDomains.foreach { domain =>
          domain.writeDomainAsYaml(DatasetArea.external)(settings.storageHandler())
        }
      } else {
        ExtractBigQuerySchema.extractAndSaveAsDomains(config, schemaHandler)
      }
    }.map(_ => JobResult.empty)
}
