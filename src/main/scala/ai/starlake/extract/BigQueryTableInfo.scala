/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.BigQuerySparkWriter
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult}
import com.google.cloud.bigquery.{Dataset, DatasetInfo, Table, TableInfo}
import com.typesafe.scalalogging.StrictLogging

import java.sql.Timestamp
import java.time.Instant
import scala.util.Try

case class BigQueryDatasetInfo(
  database: String,
  dataset: String,
  creationTime: java.sql.Timestamp,
  lastModifiedTime: java.sql.Timestamp,
  location: String,
  description: String,
  defaultTableLifetime: Long,
  timestamp: java.sql.Timestamp,
  tenant: String
)

object BigQueryDatasetInfo {
  def apply(info: DatasetInfo, logTime: java.sql.Timestamp)(implicit
    settings: Settings
  ): BigQueryDatasetInfo =
    BigQueryDatasetInfo(
      info.getDatasetId.getProject(),
      info.getDatasetId.getDataset(),
      new Timestamp(info.getCreationTime()),
      new Timestamp(info.getLastModified()),
      info.getLocation(),
      info.getDescription(),
      info.getDefaultTableLifetime(),
      logTime,
      settings.appConfig.tenant
    )
}

case class BigQueryTableInfo(
  database: String,
  dataset: String,
  table: String,
  creationTime: java.sql.Timestamp,
  expirationTime: java.sql.Timestamp,
  lastModifiedTime: java.sql.Timestamp,
  description: String,
  numBytes: Long,
  numLongTermBytes: Long,
  numRows: Long,
  requirePartitionFilter: Boolean,
  timestamp: java.sql.Timestamp,
  tenant: String
)

object BigQueryTableInfo extends StrictLogging {
  def apply(info: TableInfo, logTime: java.sql.Timestamp)(implicit
    settings: Settings
  ): BigQueryTableInfo =
    BigQueryTableInfo(
      info.getTableId.getProject(),
      info.getTableId.getDataset(),
      info.getTableId.getTable(),
      new Timestamp(info.getCreationTime()),
      new Timestamp(info.getExpirationTime),
      new Timestamp(info.getLastModifiedTime()),
      info.getDescription(),
      info.getNumBytes(),
      info.getNumLongTermBytes(),
      info.getNumRows().longValue(),
      info.getRequirePartitionFilter(),
      logTime,
      settings.appConfig.tenant
    )
  def sink(config: BigQueryTablesConfig)(implicit iSettings: Settings): Unit = {
    val logTime = java.sql.Timestamp.from(Instant.now)
    val selectedInfos: List[(Dataset, List[Table])] =
      extractTableInfos(config)

    val job = new SparkJob {
      override def name: String = "BigQueryTablesInfo"

      override implicit def settings: Settings = iSettings

      /** Just to force any job to implement its entry point using within the "run" method
        *
        * @return
        *   : Spark Dataframe for Spark Jobs None otherwise
        */
      override def run(): Try[JobResult] = Try {
        val datasetInfos = selectedInfos.map(_._1).map(BigQueryDatasetInfo(_, logTime)(settings))
        val df = session.createDataFrame(datasetInfos)
        SparkJobResult(Option(df))
      }
    }

    val jobResult = job.run()
    jobResult match {
      case scala.util.Success(SparkJobResult(Some(dfDataset), _)) =>
        BigQuerySparkWriter.sinkInAudit(
          dfDataset,
          "dataset_info",
          Some("Information related to datasets"),
          Some(BigQuerySchemaConverters.toBigQuerySchema(dfDataset.schema)),
          config.writeMode.getOrElse(WriteMode.APPEND)
        )

        val tableInfos = selectedInfos.flatMap(_._2).map(BigQueryTableInfo(_, logTime))
        val dfTable = job.session.createDataFrame(tableInfos)
        BigQuerySparkWriter.sinkInAudit(
          dfTable,
          "table_info",
          Some("Information related to tables"),
          Some(BigQuerySchemaConverters.toBigQuerySchema(dfTable.schema)),
          config.writeMode.getOrElse(WriteMode.APPEND)
        )
      case scala.util.Success(_) =>
        logger.warn("Could not extract BigQuery tables info")
      case scala.util.Failure(exception) =>
        throw new Exception("Could not extract BigQuery tables info", exception)
    }
  }

  def extractTableInfos(config: BigQueryTablesConfig)(implicit
    settings: Settings
  ): List[(Dataset, List[Table])] = {
    val infos = BigQueryInfo.extractInfo(config)
    val selectedInfos =
      if (config.tables.isEmpty)
        infos
      else {
        infos.flatMap { case (dsInfo, tableInfos) =>
          val key = config.tables.keys.find(_.equalsIgnoreCase(dsInfo.getDatasetId.getDataset))
          key match {
            case None => None
            case Some(key) =>
              val configTables = config.tables(key)
              val selectedTables =
                tableInfos.filter(tableInfo => configTables.contains(tableInfo.getTableId.getTable))
              Some((dsInfo, selectedTables))
          }
        }
      }
    selectedInfos
  }

  def run(args: Array[String]): Try[Unit] = {
    implicit val settings: Settings = Settings(Settings.referenceConfig)
    BigQueryTableInfoCmd.run(args, new SchemaHandler(settings.storageHandler())).map(_ => ())
  }
}
