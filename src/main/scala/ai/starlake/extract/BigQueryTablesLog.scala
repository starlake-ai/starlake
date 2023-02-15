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
import ai.starlake.job.sink.bigquery.{BigQueryLoadConfig, BigQuerySparkJob}
import ai.starlake.schema.generator.BigQueryTablesConfig
import ai.starlake.schema.model._
import ai.starlake.utils.Utils
import com.google.cloud.bigquery.{DatasetInfo, TableInfo}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.DataFrame

import java.sql.Timestamp
import java.time.Instant

case class BigQueryDatasetLog(
  project: String,
  dataset: String,
  creationTime: java.sql.Timestamp,
  lastModifiedTime: java.sql.Timestamp,
  location: String,
  description: String,
  defaultTableLifetime: Long,
  timestamp: java.sql.Timestamp
)

object SparkWriter extends StrictLogging {
  def sink(
    authInfo: Map[String, String],
    df: DataFrame,
    tableName: String,
    writeMode: WriteMode
  )(implicit
    settings: Settings
  ): Any = {
    settings.comet.tableInfo.sink match {
      case sink: BigQuerySink =>
        val source = Right(Utils.setNullableStateOfColumn(df, nullable = true))
        val (createDisposition, writeDisposition) = {
          Utils.getDBDisposition(writeMode, hasMergeKeyDefined = false)
        }
        val bqLoadConfig =
          BigQueryLoadConfig(
            authInfo.get("gcpProjectId"),
            authInfo.get("gcpSAJsonKey"),
            source = source,
            outputTable = tableName,
            outputDataset = sink.name.getOrElse("audit"),
            sourceFormat = settings.comet.defaultFormat,
            createDisposition = createDisposition,
            writeDisposition = writeDisposition,
            location = sink.location,
            outputPartition = sink.timestamp,
            outputClustering = sink.clustering.getOrElse(Nil),
            days = sink.days,
            requirePartitionFilter = sink.requirePartitionFilter.getOrElse(false),
            rls = Nil,
            options = sink.getOptions,
            acl = Nil
          )
        val result = new BigQuerySparkJob(bqLoadConfig, None).run()
        Utils.logFailure(result, logger)
        result.isSuccess
      case _: EsSink =>
        // TODO Sink Audit Log to ES
        throw new Exception("Sinking Audit log to Elasticsearch not yet supported")
      case _: NoneSink | FsSink(_, _, _, _, _, _) =>
      // Do nothing dataset already sinked to file. Forced at the reference.conf level
      case _ =>
    }
  }
}

object BigQueryDatasetLog {
  def apply(info: DatasetInfo, logTime: java.sql.Timestamp): BigQueryDatasetLog =
    BigQueryDatasetLog(
      info.getDatasetId.getProject(),
      info.getDatasetId.getDataset(),
      new Timestamp(info.getCreationTime()),
      new Timestamp(info.getLastModified()),
      info.getLocation(),
      info.getDescription(),
      info.getDefaultTableLifetime(),
      logTime
    )
}

case class BigQueryTableLog(
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
  timestamp: java.sql.Timestamp
)

object BigQueryTableLog {
  def apply(info: TableInfo, logTime: java.sql.Timestamp): BigQueryTableLog =
    BigQueryTableLog(
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
      logTime
    )
  def sink(config: BigQueryTablesConfig)(implicit settings: Settings) = {
    val logTime = java.sql.Timestamp.from(Instant.now)
    val authInfo = scala.collection.mutable.Map[String, String]()
    config.gcpProjectId match {
      case Some(prj) => authInfo += ("gcpProjectId" -> prj)
      case None      =>
    }
    config.gcpSAJsonKey match {
      case Some(prj) => authInfo += ("gcpSAJsonKey" -> prj)
      case None      =>
    }
    val infos = BigQueryInfo.extractProjectInfo(config.gcpProjectId)
    val datasetInfos = infos.map(_._1).map(BigQueryDatasetLog(_, logTime))
    val session = new SparkEnv("").session
    val dfDataset = session.createDataFrame(datasetInfos)
    SparkWriter.sink(
      authInfo.toMap,
      dfDataset,
      "dataset_info",
      config.writeMode.getOrElse(WriteMode.OVERWRITE)
    )
    val tableInfos = infos.flatMap(_._2).map(BigQueryTableLog(_, logTime))
    val dfTable = session.createDataFrame(tableInfos)
    SparkWriter.sink(
      authInfo.toMap,
      dfTable,
      "table_info",
      config.writeMode.getOrElse(WriteMode.OVERWRITE)
    )
  }

  def run(args: Array[String]) = {
    implicit val settings: Settings = Settings(ConfigFactory.load())
    val config =
      BigQueryTablesConfig
        .parse(args.toSeq)
        .getOrElse(throw new Exception("Could not parse arguments"))
    sink(config)
  }
}
