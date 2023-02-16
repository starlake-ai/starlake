package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Domain, Schema}
import ai.starlake.utils.YamlSerializer
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.BigQuery.{DatasetListOption, TableListOption}
import com.google.cloud.bigquery.{Dataset, StandardTableDefinition, Table}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{StructField, StructType}

import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

class BigQuery2Yml(config: BigQueryTablesConfig)(implicit settings: Settings) {
  val bigquery = config.bigquery()
  def extractDatasets(): List[Domain] = {
    val datasets = bigquery.listDatasets(DatasetListOption.pageSize(10000))
    val allDatasets = datasets
      .iterateAll()
      .asScala

    val datasetsToExtract = config.tables.keys.toList
    val filteredDatasets =
      if (config.tables.isEmpty)
        allDatasets
      else
        allDatasets.filter(ds => datasetsToExtract.contains(ds.getDatasetId.getDataset()))

    filteredDatasets.map { dataset =>
      extractDataset(dataset)
    }.toList
  }

  def extractDataset(datasetId: String): Domain = {
    extractDataset(bigquery.getDataset(datasetId))
  }

  def extractDataset(dataset: Dataset): Domain = {
    val datasetId = dataset.getDatasetId()
    val bqTables = bigquery.listTables(datasetId, TableListOption.pageSize(10000))
    val allTables = bqTables.iterateAll.asScala
    val datasetName = dataset.getDatasetId.getDataset()
    val tables =
      config.tables.get(datasetName) match {
        case Some(tables) =>
          allTables.filter(t =>
            config.tables(dataset.getDatasetId.getDataset()).contains(t.getTableId.getTable())
          )
        case None => allTables
      }

    val schemas = tables.flatMap { bqTable =>
      // We get the Table again below because Tables are returned with a null definition by listTables above.
      val tableWithDefinition = bigquery.getTable(bqTable.getTableId())
      if (tableWithDefinition.getDefinition().isInstanceOf[StandardTableDefinition])
        Some(extractTable(tableWithDefinition))
      else
        None
    }
    Domain(
      name = dataset.getDatasetId().getDataset(),
      tables = schemas.toList,
      comment = Option(dataset.getDescription)
    )
  }

  def extractTable(datasetId: String, tableId: String): Schema =
    extractTable(bigquery.getTable(datasetId, tableId))

  def extractTable(table: Table): Schema = {
    val bqSchema =
      table.getDefinition[StandardTableDefinition].getSchema
    val sparkSchema: StructType = BigQuerySchemaConverters.toSpark(bqSchema)
    val schema =
      Schema.fromSparkSchema(table.getTableId().getTable(), StructField("ignore", sparkSchema))
    schema.copy(comment = Option(table.getDescription()))
  }
}

object BigQuery2Yml {
  def extractDatasets(
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Map[String, List[Domain]] = {
    val externalSources = schemaHandler.externalSources()
    externalSources.map { external =>
      val config =
        BigQueryTablesConfig(gcpProjectId = Some(external.project), tables = external.toMap())
      val extractor = new BigQuery2Yml(config)
      external.project -> extractor.extractDatasets()
    }.toMap
  }
  def run(args: Array[String])(implicit settings: Settings): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    val config =
      BigQueryTablesConfig
        .parse(args.toSeq)
        .getOrElse(throw new Exception("Could not parse arguments"))
    val domains = new BigQuery2Yml(config).extractDatasets()
    domains.foreach { domain =>
      val domainYaml = YamlSerializer.serialize(domain)
      if (settings.storageHandler.exists(DatasetArea.external))
        settings.storageHandler.delete(DatasetArea.external)
      settings.storageHandler.write(domainYaml, DatasetArea.external)
    }
  }
}
