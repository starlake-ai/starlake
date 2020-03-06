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

package com.ebiznext.comet.schema.handlers

import java.sql.{DriverManager, SQLException}

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.{JdbcChecks, TestHelper}
import com.ebiznext.comet.job.ingest.{AuditLog, RejectedRecord}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.execution.datasources.json.JsonIngestionUtil
import org.apache.spark.sql.types._
import org.scalatest.Assertion

import scala.annotation.tailrec
import scala.util.Success

abstract class JsonIngestionJobSpecBase(variant: String) extends TestHelper with JdbcChecks {

  def expectedAuditLogs(implicit settings: Settings): List[AuditLog]

  def expectedRejectLogs(implicit settings: Settings): List[RejectedRecord]

  def configuration: Config

  ("Ingest Complex JSON " + variant) should ("should be ingested from pending to accepted, and archived ") in {
    new WithSettings(configuration) {

      new SpecTrait(
        domainFilename = "json.yml",
        sourceDomainPathname = "/sample/json/json.yml",
        datasetDomainName = "json",
        sourceDatasetPathName = "/sample/json/complex.json"
      ) {

        cleanMetadata
        cleanDatasets

        loadPending

        // Check archive
        readFileContent(cometDatasetsPath + s"/archive/${datasetDomainName}/complex.json") shouldBe loadFile(
          "/sample/json/complex.json"
        )

        // Accepted should have the same data as input
        sparkSession.read
          .parquet(
            cometDatasetsPath + s"/accepted/${datasetDomainName}/sample_json/${getTodayPartitionPath}"
          )
          .except(
            sparkSession.read
              .json(
                getClass.getResource(s"/sample/${datasetDomainName}/complex.json").toURI.getPath
              )
          )
          .count() shouldBe 0
      }
      expectingAudit("test-h2", expectedAuditLogs(settings): _*)
      expectingRejections("test-h2", expectedRejectLogs(settings): _*)
    }
  }
}

class JsonIngestionJobNoIndexNoMetricsNoAuditSpec
    extends JsonIngestionJobSpecBase("No Index, No Metrics, No Audit") {
  override def configuration: Config =
    ConfigFactory
      .parseString("""
          |audit.index.type = "None"
          |audit.index.jdbc-connection = "test-h2"
          |""".stripMargin)
      .withFallback(super.testConfiguration)

  override def expectedAuditLogs(implicit settings: Settings): List[AuditLog] = Nil

  override def expectedRejectLogs(implicit settings: Settings): List[RejectedRecord] = Nil
}

class JsonIngestionJobSpecNoIndexJdbcMetricsJdbcAuditSpec
    extends JsonIngestionJobSpecBase("No Index, Jdbc Metrics, Jdbc Audit") {

  override def configuration: Config =
    ConfigFactory
      .parseString("""
                     |metrics {
                     |  active = true
                     |  index {
                     |    type = "Jdbc"
                     |    jdbc-connection = "test-h2"
                     |  }
                     |}
                     |
                     |audit {
                     |  active = true
                     |  index {
                     |    type = "Jdbc"
                     |    jdbc-connection = "test-h2"
                     |  }
                     |}
                     |""".stripMargin)
      .withFallback(super.testConfiguration)

  override def expectedAuditLogs(implicit settings: Settings): List[AuditLog] =
    AuditLog(
      jobid = settings.comet.jobId,
      paths = "file://" + settings.comet.datasets + "/ingesting/json/complex.json",
      domain = "json",
      schema = "sample_json",
      success = true,
      count = 1,
      countOK = 1,
      countKO = 0,
      timestamp = TestStart,
      duration = 1 /* fake */,
      message = "success"
    ) :: Nil

  override def expectedRejectLogs(implicit settings: Settings): List[RejectedRecord] =
    Nil
}
