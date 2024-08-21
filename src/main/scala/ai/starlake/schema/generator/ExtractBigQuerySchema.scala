package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.BigQueryTablesConfig
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.BigQuery.{DatasetListOption, TableListOption}
import com.google.cloud.bigquery.{Dataset, StandardTableDefinition, Table}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.types.{StructField, StructType}

import scala.jdk.CollectionConverters._
import scala.util.Try

class ExtractBigQuerySchema(config: BigQueryTablesConfig)(implicit settings: Settings)
    extends StrictLogging {
  val implicitSettings = settings
  val bqJob = new BigQueryJobBase {
    val settings = implicitSettings
    override def cliConfig: BigQueryLoadConfig = BigQueryLoadConfig(
      connectionRef = config.connectionRef,
      outputDatabase = config.database,
      accessToken = config.accessToken
    )
  }

  val bigquery = bqJob.bigquery(accessToken = config.accessToken)

  def extractSchemasAndTables(schemaHandler: SchemaHandler): Try[List[(String, List[String])]] = {
    val domains = extractDatasets(schemaHandler)
    Try {
      domains
        .sortBy(_.name)
        .map { domain =>
          domain.name -> domain.tables.map(_.name).sorted
        }
    }
  }

  def extractDatasets(schemaHandler: SchemaHandler): List[Domain] = {
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
      extractDataset(dataset, schemaHandler)
    }.toList
  }

  def extractDataset(datasetId: String, schemaHandler: SchemaHandler): Domain = {
    extractDataset(bigquery.getDataset(datasetId), schemaHandler: SchemaHandler)
  }

  def extractDataset(dataset: Dataset, schemaHandler: SchemaHandler): Domain = {
    val datasetId = dataset.getDatasetId()
    val bqTables = bigquery.listTables(datasetId, TableListOption.pageSize(10000))
    val allDatawareTables =
      bqTables.iterateAll.asScala.filter(!_.getTableId.getTable().startsWith("zztmp_"))
    val datasetName = dataset.getDatasetId.getDataset()
    val allTables =
      config.tables.get(datasetName) match {
        case None =>
          allDatawareTables
        case Some(tables) if tables.contains("*") =>
          allDatawareTables.toList
        case Some(tables) =>
          allDatawareTables
            .filter(t => tables.exists(_.equalsIgnoreCase(t.getTableId.getTable())))
            .toList
      }
    val tables =
      schemaHandler.domains().find(_.finalName == datasetName) match {
        case Some(domain) =>
          val tablesToExclude = domain.tables.map(_.finalName)
          allTables.filterNot(t => tablesToExclude.contains(t.getTableId.getTable()))
        case None => allTables
      }
    val schemas = tables.flatMap { bqTable =>
      logger.info(s"Extracting table $datasetName.${bqTable.getTableId.getTable()}")
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
  def run(
    args: Array[String]
  )(implicit settings: Settings, schemaHandler: SchemaHandler): Try[Unit] = {
    ExtractBigQuerySchemaCmd.run(args.toIndexedSeq, schemaHandler).map(_ => ())
  }

  def extractAndSaveAsDomains(
    config: BigQueryTablesConfig,
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Unit = {
    val domains = new ExtractBigQuerySchema(config).extractDatasets(schemaHandler)
    domains.foreach { domain =>
      domain.writeDomainAsYaml(DatasetArea.external)(settings.storageHandler())
    }
  }
}
