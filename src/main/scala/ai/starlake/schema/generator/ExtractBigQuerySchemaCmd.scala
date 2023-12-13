package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.{BigQueryTablesCmd, BigQueryTablesConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult

import scala.util.Try

object ExtractBigQuerySchemaCmd extends BigQueryTablesCmd {
  override val command: String = "extract-bq-schema"

  override def run(config: BigQueryTablesConfig, schemaHandler: SchemaHandler)(implicit
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
}
