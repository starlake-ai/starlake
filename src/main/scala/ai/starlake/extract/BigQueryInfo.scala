package ai.starlake.extract

import ai.starlake.config.{GcpConnectionConfig, Settings}
import ai.starlake.schema.model.Domain
import com.google.cloud.bigquery.{BigQuery, Dataset, Table}

import scala.jdk.CollectionConverters.IterableHasAsScala

case class BigQueryConnectionConfig(
  gcpProjectId: Option[String] = None,
  gcpSAJsonKey: Option[String] = None,
  location: Option[String] = None
) extends GcpConnectionConfig

object BigQueryInfo {
  def extractProjectInfo(
    project: Option[String] = None
  )(implicit settings: Settings): List[(Dataset, List[Table])] = {
    val config = BigQueryConnectionConfig(project)
    val bigquery = config.bigquery()
    val datasets = bigquery.listDatasets()
    datasets
      .iterateAll()
      .asScala
      .map { dataset =>
        val config = BigQueryConnectionConfig(project)
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

  def extractDomainsInfo(
    domains: List[Domain]
  )(implicit settings: Settings): List[(Dataset, List[Table])] = {
    domains.map { domain =>
      val project = domain.project
      val config = BigQueryConnectionConfig(project)
      val bigquery = config.bigquery()
      extractDomainInfo(domain, bigquery)
    }
  }

  def extractDomainInfo(domain: Domain, bigquery: BigQuery): (Dataset, List[Table]) = {
    val bqDataset: Dataset = bigquery.getDataset(domain.getFinalName())
    val bqTables = domain.tables.map { table =>
      extractTableInfo(domain.getFinalName(), table.getFinalName(), bigquery)
    }
    (bqDataset, bqTables)
  }

  def extractTableInfo(domainName: String, tableName: String, bigquery: BigQuery) = {
    bigquery.getTable(domainName, tableName)
  }
}
