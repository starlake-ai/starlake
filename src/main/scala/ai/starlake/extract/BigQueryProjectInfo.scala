package ai.starlake.extract

import ai.starlake.config.{GcpConnectionConfig, Settings}
import com.google.cloud.bigquery.{Dataset, Table}

import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

case class BigQueryConnectionConfig(
  gcpProjectId: Option[String] = None,
  gcpSAJsonKey: Option[String] = None,
  location: Option[String] = None
) extends GcpConnectionConfig

object BigQueryInfo {
  def extractInfo(
    authConfig: GcpConnectionConfig
  )(implicit settings: Settings): List[(Dataset, List[Table])] = {
    val config = BigQueryConnectionConfig(
      authConfig.gcpProjectId,
      authConfig.gcpSAJsonKey,
      authConfig.location
    )
    val bigquery = config.bigquery()
    val datasets = bigquery.listDatasets()
    datasets
      .iterateAll()
      .asScala
      .map { dataset =>
        val bigquery = config.bigquery()
        val bqDataset: Dataset = bigquery.getDataset(dataset.getDatasetId)
        val tables = bigquery.listTables(dataset.getDatasetId)
        val bqTables = tables.iterateAll().asScala.map { table =>
          bigquery.getTable(table.getTableId)
        }
        (bqDataset, bqTables.toList)
      }
      .toList
  }
}
