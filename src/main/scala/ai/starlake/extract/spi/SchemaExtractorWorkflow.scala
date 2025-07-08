package ai.starlake.extract.spi

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.core.utils.{
  Current,
  DomainMelderConfig,
  Extract,
  KeepCurrentScript,
  LoadConfigMelder,
  TableAttributeMelderConfig,
  TableMelderConfig
}
import ai.starlake.extract.{ExtractSchemaConfig, OnExtract, SanitizeStrategy}
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.{DomainInfo, ExtractSchemasInfo, SchemaInfo}
import ai.starlake.utils.YamlSerde
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success}

/** Responsible for managing the schema extraction workflow. It extracts schemas based on the
  * configuration provided, processes the extracted domain and table-level metadata, and serializes
  * them into specified output storage.
  */
object SchemaExtractorWorkflow {
  def run(config: ExtractSchemaConfig, jdbcSchemas: ExtractSchemasInfo)(implicit
    settings: Settings
  ): Unit = {
    implicit val storageHandler: StorageHandler = settings.storageHandler()
    val loadConfigMelder = new LoadConfigMelder()
    val outputBasePath: Path =
      config.outputDir.map(new Path(_)).getOrElse(DatasetArea.load)
    val schemaExtractor = SchemaExtractorFactory.getExtractor(config, jdbcSchemas)
    schemaExtractor.extract().foreach { domain =>
      val outputDomainFolderPath = new Path(outputBasePath, domain.name)
      val outputDomainConfigPath = new Path(outputDomainFolderPath, "_config.sl.yml")
      val domainConfigExists = storageHandler.exists(outputDomainConfigPath)
      processDomain(
        loadConfigMelder,
        domain,
        outputDomainConfigPath,
        domainConfigExists
      )
      domain.tables.foreach { table =>
        processTable(
          loadConfigMelder,
          outputDomainFolderPath,
          domainConfigExists,
          table,
          jdbcSchemas.sanitizeAttributeName
        )
      }
    }
  }

  private def processDomain(
    loadConfigMelder: LoadConfigMelder,
    domain: DomainInfo,
    outputDomainConfigPath: Path,
    domainConfigExists: Boolean
  )(implicit storageHandler: StorageHandler): Unit = {
    val finalDomain = if (domainConfigExists) {
      val currentDomain = YamlSerde.deserializeYamlLoadConfig(
        storageHandler.read(outputDomainConfigPath),
        outputDomainConfigPath.toString,
        isForExtract = false
      ) match {
        case Failure(_)     => None
        case Success(value) => Some(value)
      }
      loadConfigMelder.meldDomain(DomainMelderConfig(), domain, currentDomain)
    } else {
      domain
    }
    YamlSerde.serializeToPath(outputDomainConfigPath, finalDomain)
  }

  private def processTable(
    loadConfigMelder: LoadConfigMelder,
    outputDomainFolderPath: Path,
    domainConfigExists: Boolean,
    table: SchemaInfo,
    sanitizeStrategy: SanitizeStrategy
  )(implicit storageHandler: StorageHandler): Unit = {
    val outputTableConfigPath = new Path(outputDomainFolderPath, table.name + ".sl.yml")
    val finalTable = if (domainConfigExists && storageHandler.exists(outputTableConfigPath)) {
      // if output doesn't contains domain file, then it certainly doesn't contains any extracted tables
      val currentTable = YamlSerde
        .deserializeYamlTables(
          storageHandler.read(outputTableConfigPath),
          outputTableConfigPath.toString
        )
        .map(_.table)
        .find(_.name == table.name)
      loadConfigMelder.meldTable(
        TableMelderConfig(),
        TableAttributeMelderConfig()
          .copy(rename = if (sanitizeStrategy == OnExtract) Extract else Current),
        KeepCurrentScript,
        table,
        currentTable
      )
    } else {
      table
    }
    YamlSerde.serializeToPath(outputTableConfigPath, finalTable)
  }
}
