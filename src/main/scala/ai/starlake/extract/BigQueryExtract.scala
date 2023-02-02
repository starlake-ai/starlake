package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.model.{Domain, Schema}
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.BigQuery.{DatasetListOption, TableListOption}
import com.google.cloud.bigquery.{Dataset, StandardTableDefinition, Table}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

class BigQueryExtract(config: BigQueryExtractConfig)(implicit settings: Settings) {
  val bigquery = config.bigquery()
  def getDatasets(projectId: String) = {
    val datasets = bigquery.listDatasets(DatasetListOption.pageSize(10000))
    datasets.iterateAll().asScala.map { dataset =>
      getDataset(dataset)

    }
  }

  def getDataset(datasetId: String): Domain = {
    getDataset(bigquery.getDataset(datasetId))
  }

  def getDataset(dataset: Dataset): Domain = {
    val datasetId = dataset.getDatasetId()
    val bqTables = bigquery.listTables(datasetId, TableListOption.pageSize(10000))
    val tables = bqTables.iterateAll.asScala.flatMap { bqTable =>
      // We get the Table again below because Tables are returned with a null definition by listTables above.
      val tableWithDefinition = bigquery.getTable(bqTable.getTableId())
      if (tableWithDefinition.getDefinition().isInstanceOf[StandardTableDefinition])
        Some(getTable(tableWithDefinition))
      else
        None
    }
    Domain(
      name = dataset.getDatasetId().getDataset(),
      tables = tables.toList,
      comment = Option(dataset.getDescription)
    )
  }

  def getTable(datasetId: String, tableId: String): Schema = {
    getTable(bigquery.getTable(datasetId, tableId))
  }

  def getTable(table: Table): Schema = {

    val bqSchema =
      table.getDefinition[StandardTableDefinition].getSchema
    val sparkSchema: StructType = BigQuerySchemaConverters.toSpark(bqSchema)
    val schema =
      Schema.fromSparkSchema(table.getTableId().getTable(), StructField("ignore", sparkSchema))
    schema.copy(comment = Option(table.getDescription()))
  }
}
