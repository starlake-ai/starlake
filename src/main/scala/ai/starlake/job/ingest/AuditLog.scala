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

package ai.starlake.job.ingest

import ai.starlake.job.sink.bigquery.BigQueryNativeJob
import ai.starlake.job.sink.jdbc.ConnectionLoadConfig
import ai.starlake.schema.model.{BigQuerySink, EsSink, JdbcSink, NoneSink}
import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.{BigQueryLoadConfig, BigQueryNativeJob}
import ai.starlake.job.sink.jdbc.{ConnectionLoadConfig, ConnectionLoadJob}
import ai.starlake.schema.model._
import ai.starlake.utils.{FileLock, Utils}
import com.google.cloud.bigquery.{Field, Schema => BQSchema, StandardSQLTypeName}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp
import java.text.SimpleDateFormat

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

  override def toString(): String =
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

  def asBqInsert(table: String): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val timestampStr = df.format(timestamp)
    s"""
       |insert into $table(
       | jobid, 
       | paths,
       | domain,
       | schema,
       | success, 
       | count, 
       | countAccepted, 
       | countRejected, 
       | timestamp, 
       | duration, 
       | message, 
       | step
       |) 
       |values( 
       |'$jobid',
       |'$paths',
       |'$domain',
       |'$schema',
       |$success,
       |$count,
       |$countAccepted,
       |$countRejected,
       |'$timestampStr',
       |$duration,
       |'$message',
       |'$step'
       |)""".stripMargin
  }

}

object AuditLog extends StrictLogging {

  val auditCols = List(
    ("jobid", StandardSQLTypeName.STRING, StringType),
    ("paths", StandardSQLTypeName.STRING, StringType),
    ("domain", StandardSQLTypeName.STRING, StringType),
    ("schema", StandardSQLTypeName.STRING, StringType),
    ("success", StandardSQLTypeName.BOOL, BooleanType),
    ("count", StandardSQLTypeName.INT64, LongType),
    ("countAccepted", StandardSQLTypeName.INT64, LongType),
    ("countRejected", StandardSQLTypeName.INT64, LongType),
    ("timestamp", StandardSQLTypeName.TIMESTAMP, TimestampType),
    ("duration", StandardSQLTypeName.INT64, LongType),
    ("message", StandardSQLTypeName.STRING, StringType),
    ("step", StandardSQLTypeName.STRING, StringType)
  )

  private def bqSchema(): BQSchema = {
    val fields = auditCols.map { case (name, tpe, _) =>
      Field
        .newBuilder(name, tpe)
        .setMode(Field.Mode.NULLABLE)
        .setDescription("")
        .build()
    }
    BQSchema.of(fields: _*)
  }

  def sink(session: SparkSession, log: AuditLog)(implicit
    settings: Settings
  ): Any = {
    import session.implicits._

    def sinkToFile(log: AuditLog, settings: Settings): Unit = {
      val lockPath = new Path(settings.comet.audit.path, s"audit.lock")
      val locker = new FileLock(lockPath, settings.storageHandler)
      locker.doExclusively() {
        val auditPath = new Path(settings.comet.audit.path, s"ingestion-log")
        val dfWriter = Seq(log).toDF.write.mode(SaveMode.Append)
        logger.info(s"Saving audit to path $auditPath")
        if (settings.comet.hive) {
          val hiveDB = settings.comet.audit.sink.name.getOrElse("audit")
          val tableName = "audit"
          val fullTableName = s"$hiveDB.$tableName"
          session.sql(s"create database if not exists $hiveDB")
          session.sql(s"use $hiveDB")
          logger.info(s"Saving audit to table $fullTableName")
          dfWriter
            .format(settings.comet.defaultAuditWriteFormat)
            .saveAsTable(fullTableName)
        } else {
          logger.info(s"Saving audit to file $auditPath")
          dfWriter
            .format(settings.comet.defaultAuditWriteFormat)
            .option("path", auditPath.toString)
            .save()
        }
      }
    }

    // We sink to a file when running unit tests
    if (settings.comet.sinkToFile) {
      sinkToFile(log, settings)
    }

    settings.comet.audit.sink match {
      case sink: JdbcSink =>
        val auditTypedRDD: RDD[AuditLog] = session.sparkContext.parallelize(Seq(log))
        val auditDF = session
          .createDataFrame(
            auditTypedRDD.toDF().rdd,
            StructType(
              auditCols.map { case (name, _, sparkType) =>
                StructField(name = name, dataType = sparkType, nullable = true)
              }
            )
          )
          .toDF(auditCols.map { case (name, _, _) => name }: _*)
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
        val auditDataset = sink.name.getOrElse("audit")
        BigQueryNativeJob.createTable(auditDataset, "audit", bqSchema())
        val bqConfig = BigQueryLoadConfig(
          Left("ignore"),
          outputDataset = auditDataset,
          outputTable = "audit",
          None,
          Nil,
          settings.comet.defaultFormat,
          "CREATE_IF_NEEDED",
          "WRITE_APPEND",
          None,
          None,
          options = sink.getOptions
        )
        val res = new BigQueryNativeJob(
          bqConfig,
          log.asBqInsert(bqConfig.outputDataset + "." + bqConfig.outputTable),
          None
        )
          .runBatchQuery()
        Utils.logFailure(res, logger)
      case _: EsSink =>
        // TODO Sink Audit Log to ES
        throw new Exception("Sinking Audit log to Elasticsearch not yet supported")
      case _: NoneSink | FsSink(_, _, _, _, _, _) if !settings.comet.sinkToFile =>
        sinkToFile(log, settings)
      case _: NoneSink | FsSink(_, _, _, _, _, _) if settings.comet.sinkToFile =>
      // Do nothing dataset already sinked to file. Forced at the reference.conf level
    }
  }
}
