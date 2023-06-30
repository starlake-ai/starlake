package ai.starlake.extract

import ai.starlake.config.{GcpConnectionConfig, Settings}
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig}
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
    val bqJob = new BigQueryJobBase {
      override def cliConfig: BigQueryLoadConfig = new BigQueryLoadConfig(
        gcpProjectId = authConfig.gcpProjectId,
        gcpSAJsonKey = authConfig.gcpSAJsonKey,
        location = authConfig.location,
        outputDatabase = None
      )
    }
    val bigquery = bqJob.bigquery()
    val datasets = bigquery.listDatasets()
    datasets
      .iterateAll()
      .asScala
      .map { dataset =>
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
