package com.ebiznext.comet.job.index.bqload

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.model.UserType
import com.ebiznext.comet.utils.{SparkJob, Utils}
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  Clustering,
  QueryJobConfiguration,
  TableInfo,
  UserDefinedFunction,
  ViewDefinition
}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.util.Try

class BigQueryNativeJob(
  override val cliConfig: BigQueryLoadConfig,
  sql: String,
  udf: Option[String]
)(implicit val settings: Settings)
    extends SparkJob
    with BigQueryJobBase {

  override def name: String = s"bqload-${cliConfig.outputDataset}-${cliConfig.outputTable}"

  override val projectId: String = ServiceOptions.getDefaultProjectId

  val conf: Configuration = session.sparkContext.hadoopConfiguration
  logger.info(s"BigQuery Config $cliConfig")

  def runNativeConnector(): Try[Option[DataFrame]] = {
    Try {
      val queryConfig: QueryJobConfiguration.Builder =
        QueryJobConfiguration
          .newBuilder(sql)
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
      None
    }
  }

  def runSQL(
    sql: String
  ): Unit = {
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

  }

  protected def revokeAllPrivileges(): String = {
    import cliConfig._
    s"DROP ALL ROW ACCESS POLICIES ON $outputDataset.$outputTable"
  }

  protected def grantPrivileges(): String = {
    import cliConfig._
    val rlsRetrieved = rls.getOrElse(throw new Exception("Should never happen"))
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
      |  $outputDataset.$outputTable
      | GRANT TO
      |  (${grants.mkString("\"", "\",\"", "\"")})
      | FILTER USING
      |  ($filter)
      |""".stripMargin
  }

  /**
    * Just to force any spark job to implement its entry point within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): Try[Option[DataFrame]] = {
    val res = runNativeConnector()
    Utils.logFailure(res, logger)
  }

}

object BigQueryNativeJob {

  def createViews(views: Map[String, String], udf: Option[String]) = {
    val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService

    def execute(queryConfig: QueryJobConfiguration.Builder) = {
      val results = bigquery.query(queryConfig.build())
      System.out.println(
        s"Query large results performed successfully: ${results.getTotalRows} rows inserted."
      )
    }
    views.foreach {
      case (key, value) =>
        val viewQuery: ViewDefinition.Builder =
          ViewDefinition.newBuilder(value).setUseLegacySql(false)
        val viewDefinition = udf
          .map { udf =>
            viewQuery
              .setUserDefinedFunctions(List(UserDefinedFunction.fromUri(udf)).asJava)
          }
          .getOrElse(viewQuery)
        val tableId = BigQueryJobBase.extractProjectDatasetAndTable(key)
        bigquery.create(TableInfo.of(tableId, viewDefinition.build()))
    }
  }
}
