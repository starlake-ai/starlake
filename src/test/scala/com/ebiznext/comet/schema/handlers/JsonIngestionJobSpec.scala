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

class JsonIngestionJobSpec extends TestHelper with JdbcChecks {

  "Parse valid json" should "succeed" in {
    val json =
      """
        |{
        |    "glossary": {
        |        "title": "example glossary",
        |		"GlossDiv": {
        |            "title": "S",
        |			"GlossList": {
        |                "GlossEntry": {
        |                    "ID": "SGML",
        |					"SortAs": "SGML",
        |					"GlossTerm": "Standard Generalized Markup Language",
        |					"Acronym": "SGML",
        |					"Abbrev": "ISO 8879:1986",
        |					"GlossDef": {
        |                        "para": "A meta-markup language, used to create markup languages such as DocBook.",
        |						"GlossSeeAlso": ["GML", "XML"],
        |           "IntArray":[1, 2]
        |                    },
        |					"GlossSee": "markup"
        |                }
        |            }
        |        }
        |    }
        |}
      """.stripMargin

    JsonIngestionUtil.parseString(json) shouldBe Success(
      StructType(
        Seq(
          StructField(
            "glossary",
            StructType(
              Seq(
                StructField("title", StringType, true),
                StructField(
                  "GlossDiv",
                  StructType(
                    Seq(
                      StructField("title", StringType, true),
                      StructField(
                        "GlossList",
                        StructType(
                          Seq(
                            StructField(
                              "GlossEntry",
                              StructType(
                                Seq(
                                  StructField("ID", StringType, true),
                                  StructField("SortAs", StringType, true),
                                  StructField("GlossTerm", StringType, true),
                                  StructField("Acronym", StringType, true),
                                  StructField("Abbrev", StringType, true),
                                  StructField(
                                    "GlossDef",
                                    StructType(
                                      Seq(
                                        StructField("para", StringType, true),
                                        StructField(
                                          "GlossSeeAlso",
                                          ArrayType(StringType, true),
                                          true
                                        ),
                                        StructField("IntArray", ArrayType(LongType, true), true)
                                      )
                                    ),
                                    true
                                  ),
                                  StructField("GlossSee", StringType, true)
                                )
                              ),
                              true
                            )
                          )
                        ),
                        true
                      )
                    )
                  ),
                  true
                )
              )
            ),
            true
          )
        )
      )
    )

    val json1 =
      """
        |{
        |						"GlossSeeAlso": ["GML", "XML"],
        |           "IntArray":[1.1, 2.2]
        |}
      """.stripMargin

    val json2 =
      """
        |{
        |						"GlossSeeAlso": ["GML", null],
        |           "IntArray":[1, 2]
        |}
      """.stripMargin

    JsonIngestionUtil.parseString(json1) shouldBe Success(
      StructType(
        Seq(
          StructField("GlossSeeAlso", ArrayType(StringType, true), true),
          StructField("IntArray", ArrayType(DoubleType, true), true)
        )
      )
    )

    JsonIngestionUtil.parseString(json2) shouldBe Success(
      StructType(
        Seq(
          StructField("GlossSeeAlso", ArrayType(StringType, true), true),
          StructField("IntArray", ArrayType(LongType, true), true)
        )
      )
    )

  }

  "Parse invalid json" should "fail" in {
    val json1 =
      """
        |{
        |   "complexArray": [ {"a": "Hello"}, {"a": "Hello"} ],
        |	  "GlossSeeAlso": ["GML", "XML"]
        |   "myArray":[1, 2]
        |}
      """.stripMargin

    JsonIngestionUtil.parseString(json1).isSuccess shouldBe false

  }

  def checkIngestComplexJson(
    variant: String,
    configuration: Config,
    expectedAuditLogs: Settings => List[AuditLog],
    expectedRejectLogs: Settings => List[RejectedRecord]
  ) = {
    new WithSettings(configuration) {
      ("Ingest Complex JSON " + variant) should ("should be ingested from pending to accepted, and archived " + variant) in {

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

          logger.info(
            s"thread ${Thread.currentThread().getId} ABOUT TO CHECK " + cometDatasetsPath + s"/archive/${datasetDomainName}/complex.json"
          )
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

  checkIngestComplexJson(
    "(with None sink)",
    ConfigFactory
      .parseString("""
                   |audit.index.type = "None"
                   |audit.index.jdbc-connection = "test-h2"
                   |""".stripMargin)
      .withFallback(super.testConfiguration),
    _ => Nil,
    _ => Nil
  )

  private def expectedJdbcAuditLogs(settings: Settings) =
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

  checkIngestComplexJson(
    "(with JDBC sink)",
    ConfigFactory
      .parseString("""
                   |audit.index.type = "Jdbc"
                   |audit.index.jdbc-connection = "test-h2"
                   |""".stripMargin)
      .withFallback(super.testConfiguration),
    expectedJdbcAuditLogs,
    _ => Nil
  )
}
