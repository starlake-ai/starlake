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

import ai.starlake.config.{Settings, SparkEnv}
import ai.starlake.job.sink.bigquery.BigQuerySparkWriter
import ai.starlake.schema.generator.BigQueryTablesConfig
import ai.starlake.schema.model._
import com.google.cloud.bigquery.{Dataset, DatasetInfo, Table, TableInfo}
import com.typesafe.config.ConfigFactory

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

case class BigQueryDatasetInfo(
  project: String,
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
      settings.comet.tenant
    )
}

case class BigQueryTableInfo(
  project: String,
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

object BigQueryTableInfo {
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
      settings.comet.tenant
    )
  def sink(config: BigQueryTablesConfig)(implicit settings: Settings): Unit = {
    val logTime = java.sql.Timestamp.from(Instant.now)
    val selectedInfos: List[(Dataset, List[Table])] =
      extractTableInfos(config.gcpProjectId, config.tables)

    val datasetInfos = selectedInfos.map(_._1).map(BigQueryDatasetInfo(_, logTime))
    val session = new SparkEnv("BigQueryTablesInfo-" + UUID.randomUUID().toString).session
    val dfDataset = session.createDataFrame(datasetInfos)
    BigQuerySparkWriter.sink(
      config.authInfo(),
      dfDataset,
      "dataset_info",
      Some("Information related to datasets"),
      config.writeMode.getOrElse(WriteMode.OVERWRITE)
    )
    val tableInfos = selectedInfos.flatMap(_._2).map(BigQueryTableInfo(_, logTime))
    val dfTable = session.createDataFrame(tableInfos)
    BigQuerySparkWriter.sink(
      config.authInfo(),
      dfTable,
      "table_info",
      Some("Information related to tables"),
      config.writeMode.getOrElse(WriteMode.OVERWRITE)
    )
  }

  def extractTableInfos(gcpProjectId: Option[String], tables: Map[String, List[String]])(implicit
    settings: Settings
  ): List[(Dataset, List[Table])] = {
    val infos = BigQueryInfo.extractInfo(gcpProjectId)
    val selectedInfos =
      if (tables.isEmpty)
        infos
      else {
        infos.flatMap { case (dsInfo, tableInfos) =>
          val key = tables.keys.find(_.equalsIgnoreCase(dsInfo.getDatasetId.getDataset))
          key match {
            case None => None
            case Some(key) =>
              val configTables = tables(key)
              val selectedTables =
                tableInfos.filter(tableInfo => configTables.contains(tableInfo.getTableId.getTable))
              Some((dsInfo, selectedTables))
          }
        }
      }
    selectedInfos
  }

  def run(args: Array[String]): Unit = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    val config =
      BigQueryTablesConfig.parse(args).getOrElse(throw new Exception("Could not parse arguments"))
    sink(config)
  }
}
