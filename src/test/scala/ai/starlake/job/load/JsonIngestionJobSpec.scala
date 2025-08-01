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

package ai.starlake.job.load

import ai.starlake.config.Settings
import ai.starlake.job.ingest._
import ai.starlake.{JdbcChecks, TestHelper}
import better.files.File
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, input_file_name, regexp_extract}

abstract class JsonIngestionJobSpecBase(variant: String, jsonData: String)
    extends TestHelper
    with JdbcChecks {

  def expectedAuditLogs(implicit settings: Settings): List[AuditLog]

  def expectedRejectRecords(implicit settings: Settings): List[RejectedRecord]

  def expectedMetricRecords(implicit
    settings: Settings
  ): (List[ContinuousMetricRecord], List[DiscreteMetricRecord], List[FrequencyMetricRecord])

  def configuration: Config
  new WithSettings(configuration) {
    ("Ingest Complex JSON " + variant) should "be ingested from pending to accepted, and archived " in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/json/json.sl.yml",
        datasetDomainName = "json",
        sourceDatasetPathName = "/sample/json/" + jsonData
      ) {

        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable("/sample/json/sample_json.sl.yml")
        loadPending

        // Check archive
        readFileContent(
          starlakeDatasetsPath + s"/archive/${datasetDomainName}/$jsonData"
        ) shouldBe loadTextFile(
          s"/sample/json/$jsonData"
        )

        val schemaHandler = settings.schemaHandler()
        val schema = schemaHandler.table("json", "sample_json").get
        val sparkSchema = schema.sourceSparkSchemaWithoutScriptedFields(schemaHandler)

        val location = getTablePath("json", "sample_json")
        // Accepted should have the same data as input
        val resultDf =
          sparkSession.read
            .format(settings.appConfig.defaultWriteFormat)
            .load(location)
            .where(getTodayPartitionCondition)

        val expectedDf = sparkSession.read
          .schema(sparkSchema)
          .json(
            File(getClass.getResource(s"/sample/${datasetDomainName}/$jsonData")).pathAsString
          )
          .withColumn("email_domain", regexp_extract(col("email"), ".+@(.+)", 1))
          .withColumn("source_file_name", regexp_extract(input_file_name(), ".+\\/(.+)$", 1))

        logger.info(resultDf.showString())
        logger.info(expectedDf.showString())
        resultDf
          .drop(col("millis"), col("year"), col("month"), col("day"))
          .except(
            expectedDf.drop(col("millis"))
          )
          .count() shouldBe 0

        val session = sparkSession
        import session.implicits._
        val (seconds, millis) =
          resultDf.select(col("seconds"), col("millis")).as[(String, String)].head()
        seconds shouldBe millis
      }
      expectingAudit("test-pg", expectedAuditLogs(settings): _*)
      expectingRejections("test-pg", expectedRejectRecords(settings): _*)
      val (continuous, discrete, frequencies) = expectedMetricRecords(settings)
      expectingMetrics("test-pg", continuous, discrete, frequencies)
      sparkSession.sql("DROP TABLE IF EXISTS json.sample_json")
    }

    "Ingestion JSON Schema" should "succeed" in {}
    ("Ingest JSON with unordered scripted fields " + variant) should "fail" in {

      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/json/json-invalid-script.sl.yml",
        datasetDomainName = "json",
        sourceDatasetPathName = s"/sample/json/$jsonData"
      ) {

        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable("/sample/json/sample_json.sl.yml")
        val res = loadPending
        res.getOrElse(false) shouldBe false
      }
    }
  }
}

class JsonIngestionJobNoIndexNoMetricsNoAuditSpec
    extends JsonIngestionJobSpecBase(
      "No Index, No Metrics, No Audit",
      "complex.json"
    ) {

  override def configuration: Config =
    ConfigFactory
      .parseString("""
                     |audit.index.type = "None"
                     |audit.index.connectionRef = "test-pg"
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
    extends JsonIngestionJobSpecBase(
      "No Index, Jdbc Metrics, Jdbc Audit",
      "complex.json"
    ) {

  override def configuration: Config =
    ConfigFactory
      .parseString("""
                     |grouped = false
                     |
                     |metrics {
                     |  active = true
                     |}
                     |
                     |audit {
                     |  active = true
                     |  sink {
                     |    connectionRef = "test-pg"
                     |  }
                     |}
                     |""".stripMargin)
      .withFallback(super.testConfiguration)

  override def expectedAuditLogs(implicit settings: Settings): List[AuditLog] =
    AuditLog(
      jobid = sparkSession.sparkContext.applicationId,
      paths = Some(
        new Path(
          "file:///" + settings.appConfig.datasets + "/ingesting/json/complex.json"
        ).toString
      ),
      domain = "json",
      schema = "sample_json",
      success = true,
      count = 1,
      countAccepted = 1,
      countRejected = 0,
      timestamp = TestStart,
      duration = 1 /* fake */,
      message = "success",
      Step.LOAD.toString,
      settings.appConfig.getDefaultDatabase().orElse(Some("")),
      settings.appConfig.tenant,
      test = false
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

class JsonIngestionJobSpecNoIndexNoMetricsJdbcAuditSpec
    extends JsonIngestionJobSpecBase(
      "No Index, No Metrics, Jdbc Audit",
      "complexWithError.json"
    ) {

  override def configuration: Config =
    ConfigFactory
      .parseString("""
                     |grouped = false
                     |
                     |metrics {
                     |  active = true
                     |}
                     |
                     |audit {
                     |  sink {
                     |    connectionRef = "test-pg"
                     |  }
                     |}
                     |""".stripMargin)
      .withFallback(super.testConfiguration)

  override def expectedAuditLogs(implicit settings: Settings): List[AuditLog] =
    AuditLog(
      jobid = sparkSession.sparkContext.applicationId,
      paths = Some(
        new Path(
          "file:///" + settings.appConfig.datasets + "/ingesting/json/complexWithError.json"
        ).toString
      ),
      domain = "json",
      schema = "sample_json",
      success = true,
      count = 2,
      countAccepted = 1,
      countRejected = 1,
      timestamp = TestStart,
      duration = 1 /* fake */,
      message = "success",
      Step.LOAD.toString,
      settings.appConfig.getDefaultDatabase().orElse(Some("")),
      settings.appConfig.tenant,
      test = false
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
