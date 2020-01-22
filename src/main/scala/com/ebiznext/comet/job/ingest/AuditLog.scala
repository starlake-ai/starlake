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
import com.ebiznext.comet.utils.FileLock
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SaveMode, SparkSession}

case class AuditLog(
  paths: String,
  domain: String,
  schema: String,
  success: Boolean,
  count: Long,
  countOK: Long,
  countKO: Long,
  timestamp: Long,
  duration: Long,
  errorMessage: String
) {
  override def toString(): String = {
    s"""
       |paths=$paths
       |domain=$domain
       |schema=$schema
       |success=$success
       |count=$count
       |countOK=$countOK
       |countKO=$countKO
       |timestamp=$timestamp
       |duration=$duration
       |""".stripMargin.split('\n').mkString(",")
  }
}

object SparkAuditLogWriter {

  def append(session: SparkSession, log: AuditLog) = {
    val lockPath = new Path(Settings.comet.audit.path, s"audit.lock")
    val locker = new FileLock(lockPath, Settings.storageHandler)
    if (Settings.comet.audit.active && locker.tryLock()) {
      import session.implicits._
      val auditPath = new Path(Settings.comet.audit.path, s"ingestion-log")
      Seq(log).toDF.write
        .mode(SaveMode.Append)
        .format(Settings.comet.writeFormat)
        .option("path", auditPath.toString)
        .save()
    }
  }

}
