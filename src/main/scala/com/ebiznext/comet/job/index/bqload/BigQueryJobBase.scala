package com.ebiznext.comet.job.index.bqload

import com.ebiznext.comet.schema.model.UserType
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, Dataset, DatasetId, DatasetInfo, Job, JobId, JobInfo, QueryJobConfiguration, TableId, TimePartitioning}
import com.typesafe.scalalogging.StrictLogging

trait BigQueryJobBase extends StrictLogging {
  def cliConfig: BigQueryLoadConfig
  def projectId: String

  def prepareRLS(): List[String] = {
    def revokeAllPrivileges(): String = {

      s"DROP ALL ROW ACCESS POLICIES ON ${cliConfig.outputDataset}.${cliConfig.outputTable}"
    }

    def grantPrivileges(): String = {
      val rlsRetrieved = cliConfig.rls.getOrElse(throw new Exception("Should never happen"))
      val grants = rlsRetrieved.grantees().map {
        case (UserType.SA, u) =>
          s"serviceAccount:$u"
        case (userOrGroupType, userOrGroupName) =>
          s"${userOrGroupType.toString.toLowerCase}:$userOrGroupName"
      }

      val name = rlsRetrieved.name
      val filter = rlsRetrieved.predicate
      s"""
         | CREATE ROW ACCESS POLICY
         |  $name
         | ON
         |  ${cliConfig.outputDataset}.${cliConfig.outputTable}
         | GRANT TO
         |  (${grants.mkString("\"", "\",\"", "\"")})
         | FILTER USING
         |  ($filter)
         |""".stripMargin
    }
    cliConfig.rls.toList.flatMap { rls =>
      logger.info(s"Applying security $rls")
      val rlsDelStatement = revokeAllPrivileges()
      logger.info(s"All access policies will be deleted using $rlsDelStatement")
      val rlsCreateStatement = grantPrivileges()
      logger.info(s"All access policies will be created using $rlsCreateStatement")
      List(rlsDelStatement, rlsCreateStatement)
    }
  }

  val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService

  val tableId: TableId = BigQueryJobBase.extractProjectDatasetAndTable(
    cliConfig.outputDataset + "." + cliConfig.outputTable
  )

  val datasetId: DatasetId = {
    Option(tableId.getProject) match {
      case None =>
        DatasetId.of(projectId, cliConfig.outputDataset)
      case Some(project) =>
        DatasetId.of(project, tableId.getDataset)
    }
  }

  val bqTable = s"${cliConfig.outputDataset}.${cliConfig.outputTable}"

  def getOrCreateDataset(): Dataset = {
    val dataset = Option(bigquery.getDataset(datasetId))
    dataset.getOrElse {
      val datasetInfo = DatasetInfo
        .newBuilder(cliConfig.outputDataset)
        .setLocation(cliConfig.getLocation())
        .build
      bigquery.create(datasetInfo)
    }
  }



  def timePartitioning(
    partitionField: String,
    days: Option[Int] = None,
    requirePartitionFilter: Boolean
  ): TimePartitioning.Builder = {
    days match {
      case Some(d) =>
        TimePartitioning
          .newBuilder(TimePartitioning.Type.DAY)
          .setRequirePartitionFilter(true)
          .setField(partitionField)
          .setExpirationMs(d * 3600 * 24 * 1000L)
          .setRequirePartitionFilter(requirePartitionFilter)
      case _ =>
        TimePartitioning
          .newBuilder(TimePartitioning.Type.DAY)
          .setRequirePartitionFilter(true)
          .setField(partitionField)
          .setRequirePartitionFilter(requirePartitionFilter)
    }
  }



  def runJob(statement: String, location: String): Job = {
    import java.util.UUID
    val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService
    val jobId = JobId
      .newBuilder()
      .setJob(UUID.randomUUID.toString)
      .setLocation(location)
      .build()
    val config =
      QueryJobConfiguration
        .newBuilder(statement)
        .setUseLegacySql(false)
        .build()

    // Use standard SQL syntax for queries.
    // See: https://cloud.google.com/bigquery/sql-reference/
    val job: Job = bigquery.create(JobInfo.newBuilder(config).setJobId(jobId).build)
    job.waitFor()
  }
}

object BigQueryJobBase {
  def extractProjectDatasetAndTable(value: String): TableId = {
    def extractDatasetAndTable(str: String): (String, String) = {
      val sepIndex = str.indexOf('.')
      if (sepIndex > 0)
        (str.substring(0, sepIndex), str.substring(sepIndex + 1))
      else
        throw new Exception(s"Dataset cannot be null in BigQuery view name ($value)")
    }

    val sepIndex = value.indexOf(":")
    val (project, (dataset, table)) =
      if (sepIndex > 0)
        (Some(value.substring(0, sepIndex)), extractDatasetAndTable(value.substring(sepIndex + 1)))
      else // parquet is the default
        (None, extractDatasetAndTable(value))

    project
      .map(project => TableId.of(project, dataset, table))
      .getOrElse(TableId.of(dataset, table))
  }
}
