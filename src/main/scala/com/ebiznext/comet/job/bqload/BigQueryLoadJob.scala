package com.ebiznext.comet.job.bqload

import java.util.UUID

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.utils.SparkJob
import com.google.api.services.bigquery.model.TimePartitioning
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper
import com.google.cloud.bigquery.{BigQuery, DatasetId, DatasetInfo}
import com.google.cloud.hadoop.io.bigquery.output.{BigQueryOutputConfiguration, BigQueryTimePartitioning, IndirectBigQueryOutputFormat}
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryFileFormat}
import com.google.gson.JsonParser
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.sql.SparkSession

import scala.util.{Success, Try}

class BigQueryLoadJob(
  cliConfig: BigQueryLoadConfig,
  storageHandler: StorageHandler
) extends SparkJob {

  override def name: String = s"bqload-${cliConfig.outputTable}"

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): Try[SparkSession] = {
    val conf = session.sparkContext.hadoopConfiguration

    val projectId = conf.get("fs.gs.project.id")
    val bucket = conf.get("fs.gs.system.bucket")
    // val outputTableId = projectId + ":wordcount_dataset.wordcount_output"
    val outputTableId = s"$projectId:${cliConfig.outputDataset}.${cliConfig.outputTable}"
    // Temp output bucket that is deleted upon completion of job.
    val outputGcsPath = ("gs://" + bucket + "/tmp/" + UUID.randomUUID())
    // Temp output bucket that is deleted upon completion of job.
    val jsonPath = ("gs://" + bucket + "/tmp/" + UUID.randomUUID())
    val inputPath = cliConfig.sourceFile
    BigQueryOutputConfiguration.configureWithAutoSchema(
      conf,
      outputTableId,
      outputGcsPath,
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
      classOf[TextOutputFormat[_, _]]
    )
    conf.set(
      "mapreduce.job.outputformat.class",
      classOf[IndirectBigQueryOutputFormat[_, _]].getName
    )
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY, cliConfig.writeDisposition)
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION_KEY, cliConfig.createDisposition)
    cliConfig.outputPartition.foreach { outputPartition =>
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
        );
      conf.set(BigQueryConfiguration.OUTPUT_TABLE_PARTITIONING_KEY, timePartitioning.getAsJson)
    }
    Try {
      val bigqueryHelper = RemoteBigQueryHelper.create
      val bigquery = bigqueryHelper.getOptions().getService();
      val datasetId = DatasetId.of(projectId, cliConfig.outputDataset)
      val dataset = Option(bigquery.getDataset(datasetId))
      dataset.getOrElse {
        val datasetInfo = DatasetInfo
          .newBuilder(cliConfig.outputDataset)
          .setLocation(cliConfig.getLocation())
          .build
        bigquery.create(datasetInfo)
      }

      val sourceJson = if (cliConfig.sourceFormat.equalsIgnoreCase("parquet")) {
        val parquetDF = session.read.parquet(inputPath)
        parquetDF.write.json(jsonPath)
        jsonPath
      } else if (cliConfig.sourceFormat.equalsIgnoreCase("json")) {
        inputPath
      } else {
        throw new Exception(s"Unknown format ${cliConfig.sourceFormat}")
      }

      session.sparkContext
        .textFile(sourceJson)
        .map(text => (null, new JsonParser().parse(text).getAsJsonObject))
        .saveAsNewAPIHadoopDataset(conf)
      Settings.storageHandler.delete(new Path(sourceJson))
    }
    Success(session)
  }
}
