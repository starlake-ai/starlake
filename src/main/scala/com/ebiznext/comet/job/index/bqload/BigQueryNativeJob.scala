package com.ebiznext.comet.job.index.bqload

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.utils.{JobBase, JobResult, Utils}
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery._
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.util.Try

case class BigQueryJobResult(tableResult: scala.Option[TableResult]) extends JobResult

class BigQueryNativeJob(
  override val cliConfig: BigQueryLoadConfig,
  sql: String,
  udf: scala.Option[String]
)(implicit val settings: Settings)
    extends JobBase
    with BigQueryJobBase {

  override def name: String = s"bqload-${cliConfig.outputDataset}-${cliConfig.outputTable}"

  override val projectId: String = ServiceOptions.getDefaultProjectId

  logger.info(s"BigQuery Config $cliConfig")

  def runNativeConnector(): Try[BigQueryJobResult] = {
    Try {
      val targetDataset = getOrCreateDataset()
      val queryConfig: QueryJobConfiguration.Builder =
        QueryJobConfiguration
          .newBuilder(sql)
          .setCreateDisposition(CreateDisposition.valueOf(cliConfig.createDisposition))
          .setWriteDisposition(WriteDisposition.valueOf(cliConfig.writeDisposition))
          .setDefaultDataset(targetDataset.getDatasetId)
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
      val queryConfigWithUDF = addUDFToQueryConfig(queryConfigWithClustering)
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

    val queryConfigWithUDF = addUDFToQueryConfig(queryConfig)
    val results = bigquery.query(queryConfigWithUDF.build())
    System.out.println(
      s"Query large results performed successfully: ${results.getTotalRows} rows inserted."
    )
    BigQueryJobResult(Some(results))
  }

  private def addUDFToQueryConfig(
    queryConfig: QueryJobConfiguration.Builder
  ): QueryJobConfiguration.Builder = {
    val queryConfigWithUDF = udf
      .map { udf =>
        import scala.collection.JavaConverters._
        queryConfig.setUserDefinedFunctions(List(UserDefinedFunction.fromUri(udf)).asJava)
      }
      .getOrElse(queryConfig)
    queryConfigWithUDF
  }

  /** Just to force any spark job to implement its entry point within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    val res = runNativeConnector()
    Utils.logFailure(res, logger)
  }

}

object BigQueryNativeJob extends StrictLogging {

  @deprecated("Views are now created using the syntax WTH ... AS ...", "0.1.25")
  def createViews(views: Map[String, String], udf: scala.Option[String]) = {
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
      val viewRef = scala.Option(bigquery.getTable(tableId))
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
