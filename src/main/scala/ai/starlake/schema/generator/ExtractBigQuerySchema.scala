package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.BigQueryTablesConfig
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{BigQuerySink, Domain, Metadata, Schema}
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.BigQuery.{DatasetListOption, TableListOption}
import com.google.cloud.bigquery.{Dataset, StandardTableDefinition, Table}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter
import scala.util.Try

class ExtractBigQuerySchema(config: BigQueryTablesConfig)(implicit settings: Settings) {
  val implicitSettings = settings
  val bqJob = new BigQueryJobBase {
    val settings = implicitSettings
    override def cliConfig: BigQueryLoadConfig = new BigQueryLoadConfig(
      connectionRef = config.connectionRef,
      outputDatabase = None
    )
  }
  val bigquery = bqJob.bigquery()
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
    val allDatawareTables = bqTables.iterateAll.asScala
    val datasetName = dataset.getDatasetId.getDataset()
    val tables =
      config.tables.get(datasetName) match {
        case None =>
          allDatawareTables
        case Some(tables) if tables.contains("*") =>
          allDatawareTables
        case Some(tables) =>
          allDatawareTables.filter(t => tables.exists(_.equalsIgnoreCase(t.getTableId.getTable())))
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
      comment = Option(dataset.getDescription),
      metadata = Some(
        Metadata(sink = Some(BigQuerySink(connectionRef = config.connectionRef).toAllSinks()))
      ),
      database = Option(dataset.getDatasetId().getProject())
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

object ExtractBigQuerySchema {
  def extractDatasets(
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Map[String, List[Domain]] = {
    val externalSources = schemaHandler.externalSources()
    externalSources.map { external =>
      val config =
        BigQueryTablesConfig(tables = external.toMap())
      val extractor = new ExtractBigQuerySchema(config)
      external.project -> extractor.extractDatasets()
    }.toMap
  }
  def run(args: Array[String])(implicit settings: Settings): Try[Unit] = Try {
    implicit val settings: Settings = Settings(Settings.referenceConfig)
    val config =
      BigQueryTablesConfig
        .parse(args.toSeq)
        .getOrElse(throw new Exception("Could not parse arguments"))
    extractAndSaveTables(config)
  }

  def extractAndSaveTables(config: BigQueryTablesConfig)(implicit settings: Settings): Unit = {
    val domains = new ExtractBigQuerySchema(config).extractDatasets()
    domains.foreach { domain =>
      domain.writeDomainAsYaml(DatasetArea.external)(settings.storageHandler())
    }
  }
}
