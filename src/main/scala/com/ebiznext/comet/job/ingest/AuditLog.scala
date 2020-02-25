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
import com.ebiznext.comet.job.bqload.{BigQueryLoadConfig, BigQueryLoadJob}
import com.ebiznext.comet.utils.FileLock
import com.google.cloud.bigquery.{Field, LegacySQLTypeName}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

case class AuditLog(
  jobid: String,
  paths: String,
  domain: String,
  schema: String,
  success: Boolean,
  count: Long,
  countOK: Long,
  countKO: Long,
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
       |countOK=$countOK
       |countKO=$countKO
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
    ("countOK", LegacySQLTypeName.INTEGER, LongType),
    ("countKO", LegacySQLTypeName.INTEGER, LongType),
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

  def append(session: SparkSession, log: AuditLog)(
    implicit /* TODO: make me explicit */ settings: Settings
  ) = {
    val lockPath = new Path(Settings.comet.audit.path, s"audit.lock")
    val locker = new FileLock(lockPath, Settings.storageHandler)
    import session.implicits._
    if (Settings.comet.audit.active && locker.tryLock()) {
      val res = Try {
        val auditPath = new Path(Settings.comet.audit.path, s"ingestion-log")
        Seq(log).toDF.write
          .mode(SaveMode.Append)
          .format(Settings.comet.writeFormat)
          .option("path", auditPath.toString)
          .save()
      }
      locker.release()
      res match {
        case Success(_) =>
        case Failure(e) =>
          throw e;
      }
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

    if (Settings.comet.audit.index == "BQ") {
      val bqConfig = BigQueryLoadConfig(
        Right(auditDF),
        Settings.comet.audit.options.getOrDefault("bq-dataset", "audit"),
        "audit",
        None,
        "parquet",
        "CREATE_IF_NEEDED",
        "WRITE_APPEND",
        None,
        None
      )
      new BigQueryLoadJob(bqConfig, Some(bigqueryAuditSchema())).run()
    }
  }
}
