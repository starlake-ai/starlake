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

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.job.index.bqload.{BigQueryLoadConfig, BigQuerySparkJob}
import com.ebiznext.comet.job.index.connectionload.{ConnectionLoadConfig, ConnectionLoadJob}
import com.ebiznext.comet.schema.model.{BigQuerySink, EsSink, JdbcSink, NoneSink}
import com.ebiznext.comet.utils.FileLock
import com.google.cloud.bigquery.{Field, LegacySQLTypeName}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp
import com.google.cloud.bigquery.{Schema => BQSchema}

sealed case class Step(value: String) {
  override def toString: String = value
}

object Step {

  def fromString(value: String): Step = {
    value.toUpperCase() match {
      case "LOAD"          => Step.LOAD
      case "SINK_ACCEPTED" => Step.SINK_ACCEPTED
      case "SINK_REJECTED" => Step.SINK_REJECTED
      case "TRANSFORM"     => Step.TRANSFORM
    }
  }

  object LOAD extends Step("LOAD")

  object SINK_ACCEPTED extends Step("SINK_ACCEPTED")

  object SINK_REJECTED extends Step("SINK_REJECTED")

  object TRANSFORM extends Step("TRANSFORM")

  val steps: Set[Step] = Set(LOAD, SINK_ACCEPTED, SINK_REJECTED, TRANSFORM)
}

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
  message: String,
  step: String
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
       |step=$step
       |""".stripMargin.split('\n').mkString(",")
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
    ("message", LegacySQLTypeName.STRING, StringType),
    ("step", LegacySQLTypeName.STRING, StringType)
  )

  private def bigqueryAuditSchema(): BQSchema = {
    val fields = auditCols.map { case (name, tpe, _) =>
      Field
        .newBuilder(name, tpe)
        .setMode(Field.Mode.NULLABLE)
        .setDescription("")
        .build()
    }
    BQSchema.of(fields: _*)
  }

  def append(session: SparkSession, log: AuditLog)(implicit
    settings: Settings
  ) = {
    import session.implicits._

    def sinkToFile(log: AuditLog, settings: Settings) = {
      val lockPath = new Path(settings.comet.audit.path, s"audit.lock")
      val locker = new FileLock(lockPath, settings.storageHandler)
      locker.doExclusively() {
        val auditPath = new Path(settings.comet.audit.path, s"ingestion-log")
        Seq(log).toDF.write
          .mode(SaveMode.Append)
          .format(settings.comet.defaultAuditWriteFormat)
          .option("path", auditPath.toString)
          .save()
      }
    }

    if (settings.comet.sinkToFile)
      sinkToFile(log, settings)

    val auditTypedRDD: RDD[AuditLog] = session.sparkContext.parallelize(Seq(log))
    val auditDF = session
      .createDataFrame(
        auditTypedRDD.toDF().rdd,
        StructType(
          auditCols.map(col => StructField(name = col._1, dataType = col._3, nullable = true))
        )
      )
      .toDF(auditCols.map(_._1): _*)

    settings.comet.audit.sink match {
      case sink: JdbcSink =>
        val jdbcConfig = ConnectionLoadConfig.fromComet(
          sink.connection,
          settings.comet,
          Right(auditDF),
          "audit",
          partitions = sink.partitions.getOrElse(1),
          batchSize = sink.batchsize.getOrElse(1000),
          options = sink.getOptions
        )
        new ConnectionLoadJob(jdbcConfig).run()

      case sink: BigQuerySink =>
        val bqConfig = BigQueryLoadConfig(
          Right(auditDF),
          outputDataset = sink.name.getOrElse("audit"),
          outputTable = "audit",
          None,
          Nil,
          "parquet",
          "CREATE_IF_NEEDED",
          "WRITE_APPEND",
          None,
          None,
          options = sink.getOptions
        )
        new BigQuerySparkJob(bqConfig, Some(bigqueryAuditSchema())).run()

      case _: EsSink =>
        // TODO Sink Audit Log to ES
        throw new Exception("Sinking Audit log to Elasticsearch not yet supported")
      case _: NoneSink =>
      // this is a NOP
    }
  }
}
