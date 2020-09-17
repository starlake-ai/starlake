package com.ebiznext.comet.job.index.bqload

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.{JobBase, JobResult, Utils}
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  Clustering,
  QueryJobConfiguration,
  TableInfo,
  TableResult,
  UserDefinedFunction,
  ViewDefinition
}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.util.Try

case class BigQueryJobResult(tableResult: Option[TableResult]) extends JobResult

class BigQueryNativeJob(
  override val cliConfig: BigQueryLoadConfig,
  sql: String,
  udf: Option[String]
)(implicit val settings: Settings)
    extends JobBase
    with BigQueryJobBase {

  override def name: String = s"bqload-${cliConfig.outputDataset}-${cliConfig.outputTable}"

  override val projectId: String = ServiceOptions.getDefaultProjectId

  logger.info(s"BigQuery Config $cliConfig")

  def runNativeConnector(): Try[BigQueryJobResult] = {
    Try {
      val queryConfig: QueryJobConfiguration.Builder =
        QueryJobConfiguration
          .newBuilder(sql)
          .setCreateDisposition(CreateDisposition.valueOf(cliConfig.createDisposition))
          .setWriteDisposition(WriteDisposition.valueOf(cliConfig.writeDisposition))
          .setDefaultDataset(datasetId)
          .setAllowLargeResults(true)

      val queryConfigWithPartition = (cliConfig.outputPartition) match {
        case Some(partitionField) =>
          // Generating schema from YML to get the descriptions in BQ
          val partitioning =
            timePartitioning(partitionField, cliConfig.days, cliConfig.requirePartitionFilter)
              .build()
          queryConfig.setTimePartitioning(partitioning)
        case None =>
          queryConfig
      }
      val queryConfigWithClustering = (cliConfig.outputClustering) match {
        case Nil =>
          queryConfigWithPartition
        case fields =>
          val clustering = Clustering.newBuilder().setFields(fields.asJava).build()
          queryConfigWithPartition.setClustering(clustering)
      }
      val queryConfigWithUDF = udf
        .map { udf =>
          import scala.collection.JavaConverters._
          queryConfigWithClustering.setUserDefinedFunctions(
            List(UserDefinedFunction.fromUri(udf)).asJava
          )
        }
        .getOrElse(queryConfigWithClustering)
      val results = bigquery.query(queryConfigWithUDF.setDestinationTable(tableId).build())
      logger.info(
        s"Query large results performed successfully: ${results.getTotalRows} rows inserted."
      )
      BigQueryJobResult(Some(results))
    }
  }

  def runSQL(
    sql: String
  ): BigQueryJobResult = {
    val queryConfig: QueryJobConfiguration.Builder =
      QueryJobConfiguration
        .newBuilder(sql)
        .setAllowLargeResults(true)

    val queryConfigWithUDF = udf
      .map { udf =>
        import scala.collection.JavaConverters._
        queryConfig.setUserDefinedFunctions(List(UserDefinedFunction.fromUri(udf)).asJava)
      }
      .getOrElse(queryConfig)
    val results = bigquery.query(queryConfigWithUDF.build())
    System.out.println(
      s"Query large results performed successfully: ${results.getTotalRows} rows inserted."
    )
    BigQueryJobResult(Some(results))
  }

  /**
    * Just to force any spark job to implement its entry point within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    val res = runNativeConnector()
    Utils.logFailure(res, logger)
  }

}

object BigQueryNativeJob extends StrictLogging {

  def createViews(views: Map[String, String], udf: Option[String]) = {
    val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService
    views.foreach { case (key, value) =>
      val viewQuery: ViewDefinition.Builder =
        ViewDefinition.newBuilder(value).setUseLegacySql(false)
      val viewDefinition = udf
        .map { udf =>
          viewQuery
            .setUserDefinedFunctions(List(UserDefinedFunction.fromUri(udf)).asJava)
        }
        .getOrElse(viewQuery)
      val tableId = BigQueryJobBase.extractProjectDatasetAndTable(key)
      val viewRef = Option(bigquery.getTable(tableId))
      if (viewRef.isEmpty) {
        logger.info(s"View $tableId does not exist, creating it!")
        bigquery.create(TableInfo.of(tableId, viewDefinition.build()))
        logger.info(s"View $tableId created")
      } else {
        logger.info(s"View $tableId already exist")
      }
    }
  }
}
