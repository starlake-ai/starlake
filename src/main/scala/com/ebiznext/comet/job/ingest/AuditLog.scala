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

package com.ebiznext.comet.job.ingest

import java.sql.Timestamp

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.config.Settings.SinkSettings
import com.ebiznext.comet.job.index.bqload.{BigQueryLoadConfig, BigQueryLoadJob}
import com.ebiznext.comet.job.index.jdbcload.{JdbcLoadConfig, JdbcLoadJob}
import com.ebiznext.comet.utils.FileLock
import com.google.cloud.bigquery.{Field, LegacySQLTypeName}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

case class AuditLog(
  jobid: String,
  paths: String,
  domain: String,
  schema: String,
  success: Boolean,
  count: Long,
  countAccepted: Long,
  countRejected: Long,
  timestamp: Timestamp,
  duration: Long,
  message: String
) {

  override def toString(): String = {
    s"""
       |jobid=$jobid
       |paths=$paths
       |domain=$domain
       |schema=$schema
       |success=$success
       |count=$count
       |countAccepted=$countAccepted
       |countRejected=$countRejected
       |timestamp=$timestamp
       |duration=$duration
       |message=$message
       |       |""".stripMargin.split('\n').mkString(",")
  }
}

object SparkAuditLogWriter {

  val auditCols = List(
    ("jobid", LegacySQLTypeName.STRING, StringType),
    ("paths", LegacySQLTypeName.STRING, StringType),
    ("domain", LegacySQLTypeName.STRING, StringType),
    ("schema", LegacySQLTypeName.STRING, StringType),
    ("success", LegacySQLTypeName.BOOLEAN, BooleanType),
    ("count", LegacySQLTypeName.INTEGER, LongType),
    ("countAccepted", LegacySQLTypeName.INTEGER, LongType),
    ("countRejected", LegacySQLTypeName.INTEGER, LongType),
    ("timestamp", LegacySQLTypeName.TIMESTAMP, TimestampType),
    ("duration", LegacySQLTypeName.INTEGER, LongType),
    ("message", LegacySQLTypeName.STRING, StringType)
  )

  import com.google.cloud.bigquery.{Schema => BQSchema}

  private def bigqueryAuditSchema(): BQSchema = {
    val fields = auditCols.map { attribute =>
      Field
        .newBuilder(attribute._1, attribute._2)
        .setMode(Field.Mode.REQUIRED)
        .setDescription("")
        .build()
    }
    BQSchema.of(fields: _*)
  }

  def append(session: SparkSession, log: AuditLog)(implicit
    settings: Settings
  ) = {

    import session.implicits._
    val lockPath = new Path(settings.comet.audit.path, s"audit.lock")
    val locker = new FileLock(lockPath, settings.storageHandler)
    locker.doExclusively() {
      val auditPath = new Path(settings.comet.audit.path, s"ingestion-log")
      Seq(log).toDF.write
        .mode(SaveMode.Append)
        .format(settings.comet.defaultWriteFormat)
        .option("path", auditPath.toString)
        .save()
    }
    val auditTypedRDD: RDD[AuditLog] = session.sparkContext.parallelize(Seq(log))
    val auditDF = session
      .createDataFrame(
        auditTypedRDD.toDF().rdd,
        StructType(
          auditCols.map(col => StructField(col._1, col._3, nullable = false))
        )
      )
      .toDF(auditCols.map(_._1): _*)

    settings.comet.audit.sink match {
      case SinkSettings.Jdbc(name, partitions, batchSize) =>
        val jdbcConfig = JdbcLoadConfig.fromComet(
          name,
          settings.comet,
          Right(auditDF),
          "audit",
          partitions = partitions,
          batchSize = batchSize
        )
        new JdbcLoadJob(jdbcConfig).run()

      case SinkSettings.BigQuery(dataset) =>
        val bqConfig = BigQueryLoadConfig(
          Right(auditDF),
          outputDataset = dataset,
          outputTable = "audit",
          None,
          Nil,
          "parquet",
          "CREATE_IF_NEEDED",
          "WRITE_APPEND",
          None,
          None
        )
        new BigQueryLoadJob(bqConfig, Some(bigqueryAuditSchema())).run()

      case SinkSettings.None =>
      // this is a NOP
    }
  }
}
