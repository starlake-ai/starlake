package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.BigQuery.{DatasetListOption, TableListOption}
import com.google.cloud.bigquery.{BigQuery, Dataset, TableDefinition, TableId, TableInfo}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types.{StructField, StructType}

import java.util.Locale
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class ExtractBigQuerySchema(config: TablesExtractConfig)(implicit settings: Settings)
    extends LazyLogging {
  val implicitSettings: Settings = settings
  val bqJob: BigQueryJobBase = new BigQueryJobBase {
    val settings = implicitSettings
    override def cliConfig: BigQueryLoadConfig = BigQueryLoadConfig(
      connectionRef = config.connectionRef,
      outputDatabase = config.database,
      accessToken = config.accessToken
    )
  }

  val bigquery: BigQuery = bqJob.bigquery(accessToken = config.accessToken)

  private val tableTypes: Set[TableDefinition.Type] =
    ExtractBigQuerySchema.parseTableTypes(config.tableTypes)

  /** Extracts all schemas and table names from BigQuery. This method lists all datasets and their
    * tables, filtering out temporary tables that start with "zztmp_".
    * @return
    *   A Try containing a list of tuples, each with a dataset name and a list of table names.
    */
  def extractSchemasAndTableNames(): Try[List[(String, List[String])]] = {
    Try {
      val datasets = bigquery
        .listDatasets(DatasetListOption.pageSize(10000))
        .iterateAll()
        .asScala

      datasets.map { dataset =>
        val bqTables = bigquery
          .listTables(dataset.getDatasetId, TableListOption.pageSize(10000))
          .iterateAll()
          .asScala
          .filterNot(_.getTableId.getTable().startsWith("zztmp_"))
          .map(_.getTableId.getTable())
          .toList
          .sorted
        val datasetName = dataset.getDatasetId.getDataset()
        datasetName -> bqTables
      }.toList
    }
  }

  def extractSchemasAndTables(
    schemaHandler: SchemaHandler,
    tablesToExtract: Map[String, List[String]]
  ): List[DomainInfo] = {
    // case insensitive extraction
    val datasetNames = tablesToExtract.keys.toList
    val lowercaseDatasetNames = tablesToExtract.keys.map(_.toLowerCase()).toList
    val filteredDatasets =
      if (datasetNames.size == 1) {
        // We optimize extraction for a single dataset
        val datasetName = datasetNames.head
        val dataset = bigquery.getDataset(datasetName)
        List(dataset)
      } else {
        val datasets = bigquery.listDatasets(DatasetListOption.pageSize(10000))
        datasets
          .iterateAll()
          .asScala
          .filter(ds =>
            datasetNames.isEmpty || lowercaseDatasetNames.contains(
              ds.getDatasetId.getDataset().toLowerCase()
            )
          )
      }
    filteredDatasets.map { dataset =>
      extractDataset(schemaHandler, dataset)
    }.toList
  }

  private def extractDataset(schemaHandler: SchemaHandler, dataset: Dataset): DomainInfo = {
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
          allDatawareTables
        case Some(tables) =>
          allDatawareTables
            .filter(t => tables.exists(_.equalsIgnoreCase(t.getTableId.getTable())))
      }
    val tables = {
      val tablesToExclude = schemaHandler
        .domains()
        .filter(_.finalName.equalsIgnoreCase(datasetName))
        .flatMap(_.tables.map(_.finalName.toLowerCase()))
        .toSet
      if (tablesToExclude.isEmpty) allTables
      else allTables.filterNot(t => tablesToExclude.contains(t.getTableId.getTable().toLowerCase()))
    }
    val schemas = ExtractBigQuerySchema.extractTables(
      tables,
      tableTypes,
      // We get the Table again because Tables are returned with a null schema by listTables above.
      tableId => bigquery.getTable(tableId)
    )
    DomainInfo(
      name = dataset.getDatasetId().getDataset(),
      tables = schemas,
      comment = Option(dataset.getDescription),
      metadata = Some(
        Metadata(sink = Some(BigQuerySink(connectionRef = config.connectionRef).toAllSinks()))
      ),
      database = Option(dataset.getDatasetId().getProject())
    )
  }
}

object ExtractBigQuerySchema extends LazyLogging {

  /** BigQuery object types extracted when no table type is configured. */
  val defaultTableTypes: Set[TableDefinition.Type] = Set(TableDefinition.Type.TABLE)

  /** Table types accepted by the `tableTypes` attribute and the `--table-types` option. */
  val supportedTableTypes: Map[String, TableDefinition.Type] = List(
    TableDefinition.Type.TABLE,
    TableDefinition.Type.VIEW,
    TableDefinition.Type.MATERIALIZED_VIEW,
    TableDefinition.Type.EXTERNAL,
    TableDefinition.Type.SNAPSHOT
  ).map(tableType => tableType.name() -> tableType).toMap

  private val supportedTableTypeNames: String =
    supportedTableTypes.keys.toList.sorted.mkString(", ")

  /** Maps configured table types to BigQuery object types. Names are case insensitive and spaces
    * may be used instead of underscores ("MATERIALIZED VIEW"). Unknown names, such as JDBC only
    * types, are ignored with a warning.
    *
    * @param tableTypes
    *   configured table types, Nil to extract tables only
    * @return
    *   the BigQuery object types to extract
    */
  def parseTableTypes(tableTypes: List[String]): Set[TableDefinition.Type] = {
    if (tableTypes.isEmpty) {
      defaultTableTypes
    } else {
      val parsed = tableTypes.map { tableType =>
        tableType -> supportedTableTypes.get(
          tableType.trim.toUpperCase(Locale.ROOT).replaceAll("\\s+", "_")
        )
      }
      val unknown = parsed.collect { case (tableType, None) => tableType }
      val known = parsed.flatMap(_._2).toSet
      if (unknown.nonEmpty)
        logger.warn(
          s"Ignoring table types not supported by BigQuery: ${unknown.mkString(", ")}. Supported table types are $supportedTableTypeNames"
        )
      if (known.isEmpty)
        throw new IllegalArgumentException(
          s"No supported BigQuery table type in ${tableTypes.mkString(", ")}. Supported table types are $supportedTableTypeNames"
        )
      known
    }
  }

  /** @return
    *   true if the object type is one of the table types to extract, or is not known yet. Logs a
    *   warning when the object is skipped.
    */
  def isExtractable(table: TableInfo, tableTypes: Set[TableDefinition.Type]): Boolean = {
    Option(table.getDefinition[TableDefinition]()).flatMap(d => Option(d.getType)) match {
      case Some(tableType) if !tableTypes.contains(tableType) =>
        logger.warn(
          s"Skipping ${tableType.name()} ${table.getTableId.getDataset()}.${table.getTableId
              .getTable()}: only ${tableTypes.map(_.name()).toList.sorted.mkString(", ")} objects are extracted. Add ${tableType.name()} to the BigQuery table types to extract it (tableTypes attribute of the extract config, --table-types option of extract-bq-schema)"
        )
        false
      case _ =>
        true
    }
  }

  /** Extracts the schemas of the listed objects whose type is one of the table types to extract. An
    * object that cannot be fetched or whose schema cannot be converted is skipped with an error.
    *
    * @param listedTables
    *   objects returned by listTables: their definition holds their type but not their schema
    * @param tableTypes
    *   the BigQuery object types to extract
    * @param getTable
    *   fetches an object with its schema, null if it does not exist anymore
    */
  def extractTables(
    listedTables: Iterable[TableInfo],
    tableTypes: Set[TableDefinition.Type],
    getTable: TableId => TableInfo
  ): List[SchemaInfo] = {
    // Filter on the listed type first to avoid fetching objects that would be skipped anyway, and
    // again once fetched in case the listing did not return it.
    listedTables.filter(isExtractable(_, tableTypes)).toList.flatMap { listedTable =>
      val tableId = listedTable.getTableId
      val tableName = s"${tableId.getDataset()}.${tableId.getTable()}"
      Try(Option(getTable(tableId))) match {
        case Success(Some(table)) if isExtractable(table, tableTypes) =>
          logger.info(s"Extracting table $tableName")
          Try(extractTable(table)) match {
            case Success(schema) =>
              schema
            case Failure(e) =>
              logger.error(s"Failed to extract the schema of $tableName", e)
              None
          }
        case Success(Some(_)) =>
          None
        case Success(None) =>
          logger.warn(s"Skipping $tableName: it does not exist anymore")
          None
        case Failure(e) =>
          logger.error(s"Failed to get table $tableName", e)
          None
      }
    }
  }

  /** @return
    *   the table schema read from the table definition, whatever the BigQuery object type, or None
    *   if the definition has no schema.
    */
  def extractTable(table: TableInfo): Option[SchemaInfo] = {
    val tableName = table.getTableId().getTable()
    Option(table.getDefinition[TableDefinition]()).flatMap(d => Option(d.getSchema)) match {
      case Some(bqSchema) =>
        val sparkSchema: StructType = BigQuerySchemaConverters.toSpark(bqSchema)
        val schema = SchemaInfo.fromSparkSchema(tableName, StructField("ignore", sparkSchema))
        Some(schema.copy(comment = Option(table.getDescription())))
      case None =>
        logger.warn(
          s"Skipping ${table.getTableId.getDataset()}.$tableName: its definition has no schema"
        )
        None
    }
  }

  def run(
    args: Array[String]
  )(implicit settings: Settings, schemaHandler: SchemaHandler): Try[Unit] = {
    ExtractBigQuerySchemaCmd.run(args.toIndexedSeq, schemaHandler).map(_ => ())
  }

  def extractAndSaveToExternal(
    config: TablesExtractConfig,
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): List[DomainInfo] = {
    val domains =
      new ExtractBigQuerySchema(config).extractSchemasAndTables(schemaHandler, config.tables)
    schemaHandler.saveToExternals(domains)
    domains
  }
}
