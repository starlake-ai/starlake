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

package ai.starlake.schema.handlers

import ai.starlake.config.Settings
import ai.starlake.job.ingest._
import ai.starlake.{JdbcChecks, TestHelper}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, input_file_name, regexp_extract}

abstract class JsonIngestionJobSpecBase(variant: String) extends TestHelper with JdbcChecks {

  def expectedAuditLogs(implicit settings: Settings): List[AuditLog]

  def expectedRejectRecords(implicit settings: Settings): List[RejectedRecord]

  def expectedMetricRecords(implicit
    settings: Settings
  ): (List[ContinuousMetricRecord], List[DiscreteMetricRecord], List[FrequencyMetricRecord])

  def configuration: Config

  ("Ingest Complex JSON " + variant) should "should be ingested from pending to accepted, and archived " in {
    new WithSettings(configuration) {

      new SpecTrait(
        domainOrJobFilename = "json.comet.yml",
        sourceDomainOrJobPathname = "/sample/json/json.comet.yml",
        datasetDomainName = "json",
        sourceDatasetPathName = "/sample/json/complex.json"
      ) {

        cleanMetadata
        cleanDatasets

        loadPending

        // Check archive
        readFileContent(
          cometDatasetsPath + s"/archive/${datasetDomainName}/complex.json"
        ) shouldBe loadTextFile(
          "/sample/json/complex.json"
        )

        val schemaHandler = new SchemaHandler(settings.storageHandler)
        val schema = schemaHandler.getSchema("json", "sample_json").get
        val sparkSchema = schema.sparkSchemaWithoutScriptedFields(schemaHandler)

        // Accepted should have the same data as input
        val resultDf = sparkSession.read
          .parquet(
            cometDatasetsPath + s"/accepted/${datasetDomainName}/sample_json/${getTodayPartitionPath}"
          )

        val expectedDf = sparkSession.read
          .schema(sparkSchema)
          .json(
            getClass.getResource(s"/sample/${datasetDomainName}/complex.json").toURI.getPath
          )
          .withColumn("email_domain", regexp_extract(col("email"), ".+@(.+)", 1))
          .withColumn("source_file_name", regexp_extract(input_file_name, ".+\\/(.+)$", 1))

        logger.info(resultDf.showString())
        logger.info(expectedDf.showString())
        resultDf
          .drop(col("millis"))
          .except(
            expectedDf.drop(col("millis"))
          )
          .count() shouldBe 0

        import sparkSession.implicits._
        val (seconds, millis) =
          resultDf.select(col("seconds"), col("millis")).as[(String, String)].head()
        seconds shouldBe millis
      }
      expectingAudit("test-h2", expectedAuditLogs(settings): _*)
      expectingRejections("test-h2", expectedRejectRecords(settings): _*)
      val (continuous, discrete, frequencies) = expectedMetricRecords(settings)
      expectingMetrics("test-h2", continuous, discrete, frequencies)
    }
  }

  "Ingestion JSn Scheam" should "succceed" in {}
  ("Ingest JSON with unordered scripted fields " + variant) should "fail" in {
    new WithSettings(configuration) {

      new SpecTrait(
        domainOrJobFilename = "json.comet.yml",
        sourceDomainOrJobPathname = "/sample/json/json-invalid-script.comet.yml",
        datasetDomainName = "json",
        sourceDatasetPathName = "/sample/json/complex.json"
      ) {

        cleanMetadata
        cleanDatasets
        loadPending shouldBe false
      }
    }
  }
}

class JsonIngestionJobNoIndexNoMetricsNoAuditSpec
    extends JsonIngestionJobSpecBase("No Index, No Metrics, No Audit") {

  override def configuration: Config =
    ConfigFactory
      .parseString("""
          |audit.index.type = "None"
          |audit.index.connection = "test-h2"
          |""".stripMargin)
      .withFallback(super.testConfiguration)

  override def expectedAuditLogs(implicit settings: Settings): List[AuditLog] = Nil

  override def expectedRejectRecords(implicit settings: Settings): List[RejectedRecord] = Nil

  override def expectedMetricRecords(implicit
    settings: Settings
  ): (List[ContinuousMetricRecord], List[DiscreteMetricRecord], List[FrequencyMetricRecord]) =
    (Nil, Nil, Nil)
}

class JsonIngestionJobSpecNoIndexJdbcMetricsJdbcAuditSpec
    extends JsonIngestionJobSpecBase("No Index, Jdbc Metrics, Jdbc Audit") {

  override def configuration: Config =
    ConfigFactory
      .parseString("""
                     |grouped = false
                     |
                     |metrics {
                     |  active = true
                     |  sink {
                     |    type = "JdbcSink"
                     |    connection = "test-h2"
                     |  }
                     |}
                     |
                     |audit {
                     |  active = true
                     |  sink {
                     |    type = "JdbcSink"
                     |    connection = "test-h2"
                     |  }
                     |}
                     |""".stripMargin)
      .withFallback(super.testConfiguration)

  override def expectedAuditLogs(implicit settings: Settings): List[AuditLog] =
    AuditLog(
      jobid = sparkSession.sparkContext.applicationId,
      paths = "file:" + settings.comet.datasets + "/ingesting/json/complex.json",
      domain = "json",
      schema = "sample_json",
      success = true,
      count = 1,
      countAccepted = 1,
      countRejected = 0,
      timestamp = TestStart,
      duration = 1 /* fake */,
      message = "success",
      Step.LOAD.toString
    ) :: Nil

  override def expectedRejectRecords(implicit settings: Settings): List[RejectedRecord] =
    Nil

  override def expectedMetricRecords(implicit
    settings: Settings
  ): (List[ContinuousMetricRecord], List[DiscreteMetricRecord], List[FrequencyMetricRecord]) =
    (
      Nil,
      Nil,
      Nil
    )
}
