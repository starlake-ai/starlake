package com.ebiznext.comet.job.index.bqload

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.model.UserType
import com.ebiznext.comet.utils.conversion.BigQueryUtils._
import com.ebiznext.comet.utils.conversion.syntax._
import com.ebiznext.comet.utils.{SparkJob, SparkJobResult, Utils}
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper
import com.google.cloud.bigquery.{Schema => BQSchema, _}
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTimePartitioning
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try

class BigQueryLoadJob(
  cliConfig: BigQueryLoadConfig,
  maybeSchema: scala.Option[BQSchema] = None
)(implicit val settings: Settings)
    extends SparkJob {

  override def name: String = s"bqload-${cliConfig.outputDataset}-${cliConfig.outputTable}"

  val conf = session.sparkContext.hadoopConfiguration
  logger.info(s"BigQuery Config $cliConfig")

  val bigqueryHelper = RemoteBigQueryHelper.create
  val bigquery = bigqueryHelper.getOptions.getService

  val projectId = conf.get("fs.gs.project.id")
  val bucket = conf.get("fs.defaultFS")

  val tableId = TableId.of(cliConfig.outputDataset, cliConfig.outputTable)
  val bqTable = s"${cliConfig.outputDataset}.${cliConfig.outputTable}"

  private def getOrCreateDataset(): Dataset = {
    val datasetId = DatasetId.of(projectId, cliConfig.outputDataset)
    val dataset = scala.Option(bigquery.getDataset(datasetId))
    dataset.getOrElse {
      val datasetInfo = DatasetInfo
        .newBuilder(cliConfig.outputDataset)
        .setLocation(cliConfig.getLocation())
        .build
      bigquery.create(datasetInfo)
    }
  }

  def getOrCreateTable(df: DataFrame): Table = {
    getOrCreateDataset()
    import com.google.cloud.bigquery.{StandardTableDefinition, TableInfo}
    scala.Option(bigquery.getTable(tableId)) getOrElse {

      val tableDefinitionBuilder =
        maybeSchema match {
          case Some(schema) =>
            // Generating schema from YML to get the descriptions in BQ
            StandardTableDefinition.of(schema).toBuilder
          case None =>
            // We would have loved to let BQ do the whole job (StandardTableDefinition.newBuilder())
            // But however seems like it does not work when there is an output partition
            cliConfig.outputPartition match {
              case Some(_) => StandardTableDefinition.of(df.to[BQSchema]).toBuilder
              case None    =>
                // In case of complex types, our inferred schema does not work, BQ introduces a list subfield, let him do the dirty job
                StandardTableDefinition.newBuilder()
            }
        }

      cliConfig.outputPartition.foreach { outputPartition =>
        import com.google.cloud.bigquery.TimePartitioning
        val timeField =
          if (List("_PARTITIONDATE", "_PARTITIONTIME").contains(outputPartition))
            TimePartitioning
              .newBuilder(TimePartitioning.Type.DAY)
              .setRequirePartitionFilter(true)
          else
            TimePartitioning
              .newBuilder(TimePartitioning.Type.DAY)
              .setRequirePartitionFilter(true)
              .setField(outputPartition)

        val timeFieldWithExpiration = cliConfig.days
          .map(_ * 3600 * 24 * 1000L)
          .map(ms => timeField.setExpirationMs(ms))
          .getOrElse(timeField)
          .build()
        tableDefinitionBuilder.setTimePartitioning(timeFieldWithExpiration)
      }

      val tableDefinition = tableDefinitionBuilder.build()
      val tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build
      bigquery.create(tableInfo)
    }
  }

  def prepareConf(): Configuration = {
    val conf = session.sparkContext.hadoopConfiguration
    logger.info(s"BigQuery Config $cliConfig")
    val bucket = scala.Option(conf.get("fs.gs.system.bucket"))
    bucket.foreach { bucket =>
      logger.info(s"Temporary GCS path $bucket")
      session.conf.set("temporaryGcsBucket", bucket)
    }

    val writeDisposition = JobInfo.WriteDisposition.valueOf(cliConfig.writeDisposition)
    val finalWriteDisposition = writeDisposition match {
      case JobInfo.WriteDisposition.WRITE_TRUNCATE =>
        logger.info(s"Deleting table $tableId")
        try {
          bigquery.delete(tableId)
        } catch {
          case e: BigQueryException =>
            // Log error and continue  (may be table does not exist)
            Utils.logException(logger, e)
        }

        logger.info(s"Setting Write mode to Append")
        JobInfo.WriteDisposition.WRITE_APPEND
      case _ =>
        writeDisposition
    }

    conf.set(
      BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY,
      finalWriteDisposition.toString
    )
    conf.set(
      BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION_KEY,
      cliConfig.createDisposition
    )
    cliConfig.outputPartition.foreach { outputPartition =>
      import com.google.cloud.hadoop.repackaged.bigquery.com.google.api.services.bigquery.model.TimePartitioning
      val timeField =
        if (List("_PARTITIONDATE", "_PARTITIONTIME").contains(outputPartition))
          new TimePartitioning().setType("DAY").setRequirePartitionFilter(true)
        else
          new TimePartitioning()
            .setType("DAY")
            .setRequirePartitionFilter(true)
            .setField(outputPartition)
      val timePartitioning =
        new BigQueryTimePartitioning(
          timeField
        )
      conf.set(BigQueryConfiguration.OUTPUT_TABLE_PARTITIONING_KEY, timePartitioning.getAsJson)
    }
    conf
  }

  def prepareRLS(): List[String] = {
    cliConfig.rls.toList.flatMap { rls =>
      logger.info(s"Applying security $rls")
      val rlsDelStatement = toBQDelGrant()
      logger.info(s"All access policies will be deleted using $rlsDelStatement")
      val rlsCreateStatement = toBQGrant()
      logger.info(s"All access policies will be created using $rlsCreateStatement")
      List(rlsDelStatement, rlsCreateStatement)
    }
  }

  def runBQSparkConnector(): Try[SparkJobResult] = {
    prepareConf()
    Try {
      val sourceDF =
        cliConfig.source match {
          case Left(path) => session.read.parquet(path)
          case Right(df)  => df
        }

      val table = getOrCreateTable(sourceDF)

      val stdTableDefinition =
        bigquery.getTable(table.getTableId).getDefinition.asInstanceOf[StandardTableDefinition]
      logger.info(
        s"BigQuery Saving to  ${table.getTableId} containing ${stdTableDefinition.getNumRows} rows"
      )
      sourceDF.write
        .mode(SaveMode.Append)
        .format("com.google.cloud.spark.bigquery")
        .option("table", bqTable)
        .save()

      val stdTableDefinitionAfter =
        bigquery.getTable(table.getTableId).getDefinition.asInstanceOf[StandardTableDefinition]
      logger.info(
        s"BigQuery Saved to ${table.getTableId} now contains ${stdTableDefinitionAfter.getNumRows} rows"
      )

      prepareRLS().foreach { rlsStatement =>
        logger.info(s"Applying security $rlsStatement")
        try {
          bqJobRun(rlsStatement)
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
      SparkJobResult(session)
    }
  }

  private def bqJobRun(statement: String) = {
    import java.util.UUID

    import scala.collection.JavaConverters._
    val jobId = JobId
      .newBuilder()
      .setJob(UUID.randomUUID.toString)
      .setLocation(cliConfig.getLocation())
      .build()
    val config =
      QueryJobConfiguration
        .newBuilder(statement)
        .setUseLegacySql(false)
        .build()
    // Use standard SQL syntax for queries.
    // See: https://cloud.google.com/bigquery/sql-reference/
    val job: Job = bigquery.create(JobInfo.newBuilder(config).setJobId(jobId).build)
    scala.Option(job.waitFor()) match {
      case None =>
        throw new RuntimeException("Job no longer exists")
      case Some(job) if job.getStatus.getExecutionErrors() != null =>
        throw new RuntimeException(job.getStatus.getExecutionErrors().asScala.reverse.mkString(","))
      case Some(_) =>
      // everything went well !
    }
  }

  private def toBQDelGrant(): String = {
    import cliConfig._
    s"DROP ALL ROW ACCESS POLICIES ON $outputDataset.$outputTable"
  }

  private def toBQGrant(): String = {
    import cliConfig._
    val rlsGet = rls.getOrElse(throw new Exception("Should never happen"))
    val grants = rlsGet.grantees().map {
      case (UserType.SA, u) =>
        s"serviceAccount:$u"
      case (t, u) =>
        s"${t.toString.toLowerCase}:$u"
    }

    val name = rlsGet.name
    val filter = rlsGet.predicate
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
  override def run(): Try[SparkJobResult] = {
    val res = runBQSparkConnector()
    Utils.logFailure(res, logger)
  }
}
