package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig}
import com.google.cloud.bigquery.{Dataset, Table}

import scala.jdk.CollectionConverters._

object BigQueryInfo {
  def extractInfo(
    config: BigQueryTablesConfig
  )(implicit settings: Settings): List[(Dataset, List[Table])] = {
    val implicitSettings = settings
    val bqJob = new BigQueryJobBase {
      val settings: Settings = implicitSettings
      override def cliConfig: BigQueryLoadConfig = new BigQueryLoadConfig(
        connectionRef = config.connectionRef,
        outputDatabase = None,
        accessToken = config.accessToken
      )
    }
    val bigquery = bqJob.bigquery(accessToken = config.accessToken)
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
