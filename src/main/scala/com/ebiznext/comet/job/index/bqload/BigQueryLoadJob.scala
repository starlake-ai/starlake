package com.ebiznext.comet.job.index.bqload

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.model.UserType
import com.ebiznext.comet.utils.conversion.BigQueryUtils._
import com.ebiznext.comet.utils.conversion.syntax._
import com.ebiznext.comet.utils.{SparkJob, SparkJobResult, Utils}
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper
import com.google.cloud.bigquery.{Schema => BQSchema, _}
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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
    scala.Option(bigquery.getTable(tableId)) getOrElse {

      val withPartitionDefinition =
        (maybeSchema, cliConfig.outputPartition) match {
          case (Some(schema), Some(partitionField)) =>
            // Generating schema from YML to get the descriptions in BQ
            val partitioning =
              timePartitioning(partitionField, cliConfig.days, cliConfig.requirePartitionFilter)
                .build()
            StandardTableDefinition
              .newBuilder()
              .setSchema(schema)
              .setTimePartitioning(partitioning)
          case (Some(schema), _) =>
            // Generating schema from YML to get the descriptions in BQ
            StandardTableDefinition
              .newBuilder()
              .setSchema(schema)
          case (_, Some(partitionField)) =>
            // We would have loved to let BQ do the whole job (StandardTableDefinition.newBuilder())
            // But however seems like it does not work when there is an output partition
            val partitioning =
              timePartitioning(partitionField, cliConfig.days, cliConfig.requirePartitionFilter)
                .build()
            StandardTableDefinition
              .newBuilder()
              .setSchema(df.to[BQSchema])
              .setTimePartitioning(partitioning)
          case (_, _) =>
            // In case of complex types, our inferred schema does not work, BQ introduces a list subfield, let him do the dirty job
            StandardTableDefinition
              .newBuilder()
        }

      val withClusteringDefinition =
        cliConfig.outputClustering match {
          case Nil =>
            withPartitionDefinition
          case fields =>
            import scala.collection.JavaConverters._
            val clustering = Clustering.newBuilder().setFields(fields.asJava).build()
            withPartitionDefinition.setClustering(clustering)
        }
      val tableInfo = TableInfo.newBuilder(tableId, withClusteringDefinition.build()).build
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
    val finalWriteDisposition = (writeDisposition, cliConfig.outputPartition) match {
      case (JobInfo.WriteDisposition.WRITE_TRUNCATE, Some(partition)) =>
        logger.info(s"Overwriting partition $partition of Table $tableId")
        logger.info(s"Setting Write mode to Overwrite")
        JobInfo.WriteDisposition.WRITE_TRUNCATE
      case (JobInfo.WriteDisposition.WRITE_TRUNCATE, _) =>
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
    conf
  }

  def prepareRLS(): List[String] = {
    cliConfig.rls.toList.flatMap { rls =>
      logger.info(s"Applying security $rls")
      val rlsDelStatement = revokeAllPrivileges()
      logger.info(s"All access policies will be deleted using $rlsDelStatement")
      val rlsCreateStatement = grantPrivileges()
      logger.info(s"All access policies will be created using $rlsCreateStatement")
      List(rlsDelStatement, rlsCreateStatement)
    }
  }

  def runBQSparkConnector(): Try[SparkJobResult] = {

    prepareConf()
    Try {
      val cacheStorageLevel =
        settings.comet.internal.map(_.cacheStorageLevel).getOrElse(StorageLevel.MEMORY_AND_DISK)
      val sourceDF =
        cliConfig.source match {
          case Left(path) =>
            session.read
              .parquet(path)
              .persist(
                cacheStorageLevel
              )
          case Right(df) => df.persist(cacheStorageLevel)
        }

      val table = getOrCreateTable(sourceDF)

      val stdTableDefinition =
        bigquery.getTable(table.getTableId).getDefinition.asInstanceOf[StandardTableDefinition]
      logger.info(
        s"BigQuery Saving to  ${table.getTableId} containing ${stdTableDefinition.getNumRows} rows"
      )

      (cliConfig.writeDisposition, cliConfig.outputPartition) match {
        case ("WRITE_TRUNCATE", Some(partition)) =>
          logger.info(s"overwriting partition ${partition} in The BQ Table $bqTable")
          // BigQuery supports only this date format 'yyyyMMdd', so we have to use it
          // in order to overwrite only one partition
          val dateFormat = "yyyyMMdd"
          val partitions = sourceDF
            .select(date_format(col(partition), dateFormat).cast("string"))
            .where(col(partition).isNotNull)
            .distinct()
            .rdd
            .map(r => r.getString(0))
            .collect()

          partitions.foreach(partitionStr =>
            sourceDF
              .where(date_format(col(partition), dateFormat).cast("string") === partitionStr)
              .write
              .mode(SaveMode.Overwrite)
              .format("com.google.cloud.spark.bigquery")
              .option(
                "table",
                bqTable
              )
              .option("datePartition", partitionStr)
              .save()
          )
        case _ =>
          logger.info(s"Saving BQ Table $bqTable")
          sourceDF.write
            .mode(SaveMode.Append)
            .format("com.google.cloud.spark.bigquery")
            .option("table", bqTable)
            .save()
      }

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
        logger.info(s"Job with id ${jobId.getJob()} on Statement $statement succeeded")
    }
  }

  private def revokeAllPrivileges(): String = {
    import cliConfig._
    s"DROP ALL ROW ACCESS POLICIES ON $outputDataset.$outputTable"
  }

  private def grantPrivileges(): String = {
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
  override def run(): Try[SparkJobResult] = {
    val res = runBQSparkConnector()
    Utils.logFailure(res, logger)
  }

  def timePartitioning(
    partitionField: String,
    days: scala.Option[Int] = None,
    requirePartitionFilter: Boolean
  ): TimePartitioning.Builder =
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

object BigQueryLoadJob {

  def getTable(
    session: SparkSession,
    datasetName: String,
    tableName: String
  ): scala.Option[Table] = {
    val conf = session.sparkContext.hadoopConfiguration
    val projectId = conf.get("fs.gs.project.id")
    val bigqueryHelper = RemoteBigQueryHelper.create
    val bigquery = bigqueryHelper.getOptions.getService
    val datasetId: DatasetId = DatasetId.of(projectId, datasetName)
    scala.Option(bigquery.getDataset(datasetId)).flatMap { dataset =>
      val tableId = TableId.of(datasetName, tableName)
      scala.Option(bigquery.getTable(tableId))
    }
  }
}
